"""
Airflow building blocks used by the generated DAG / task files:

    spark_job_task()      run ONE LakeHouse_Banking Spark job and write its flags (common/flags.py):
                              docker exec spark-client-dungafk spark-submit /home/spark/jobs/<...>.py <date options>
                          flag RUNNING → spark-submit → SUCCESS (exit 0) / FAILED (task fails, Airflow retries)
    check_flags_task()    sensor: wait until every source table has a SUCCESS flag for the dates of the run
    domain_done_task()    last task of a bronze domain DAG: fails the run when any table failed

Date options (the same as the LakeHouse jobs, see LakeHouse_Banking/how_to_run.md):
    COB_DT_ONLY  bronze snapshot jobs: --cob-dt only
    DATE_RANGE   bronze incremental + silver jobs: --cob-dt, or --from-dt/--to-dt, or nothing (= full load)
    NO_DATE      jobs without date options (silver__dim_date)

Which date is passed (resolve_dates):
    1. params from_dt + to_dt → --from-dt X --to-dt Y; flags for every day X..Y.
                                COB_DT_ONLY jobs are SKIPPED (a snapshot cannot be taken for past days).
    2. param full_load = true → DATE_RANGE: no date = FULL LOAD, no flags (no single date).
                                COB_DT_ONLY: same as 4 (LakeHouse snapshot "no date" = T-1 anyway).
    3. param cob_dt           → --cob-dt X
    4. nothing (scheduled)    → --cob-dt = run date in Vietnam time - 1 day (T-1)
"""
import logging
import shlex
import subprocess
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pendulum
from airflow.sdk import Param, PokeReturnValue, task

try:  # Airflow 3 Task SDK
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:  # pragma: no cover
    from airflow.exceptions import AirflowSkipException

from common import flags
from common.settings import FLAG_POKE_INTERVAL, FLAG_WAIT_TIMEOUT, LOCAL_TZ, SPARK_CLIENT_CONTAINER, SPARK_POOL

log = logging.getLogger(__name__)

COB_DT_ONLY = "cob_dt_only"
DATE_RANGE = "date_range"
NO_DATE = "no_date"


# ── DAG params ──────────────────────────────────────────────────────────────────────────────────────────────
def date_params(date_mode: str) -> Dict[str, Param]:
    """DAG params (form of "Trigger DAG w/ config") that match the date options of the job(s)."""
    if date_mode == NO_DATE:
        return {}
    params = {
        "cob_dt": Param(None, type=["null", "string"], format="date", title="cob_dt",
                        description="Business date to load (YYYY-MM-DD). Empty = run date - 1 day."),
    }
    if date_mode == DATE_RANGE:
        params["from_dt"] = Param(None, type=["null", "string"], format="date", title="from_dt",
                                  description="Backfill start (YYYY-MM-DD), together with to_dt. Wins over cob_dt. "
                                              "Snapshot tables are skipped.")
        params["to_dt"] = Param(None, type=["null", "string"], format="date", title="to_dt",
                                description="Backfill end (YYYY-MM-DD), together with from_dt.")
        params["full_load"] = Param(False, type="boolean", title="full_load",
                                    description="Run with no date = FULL LOAD (ignores the dates above, writes no flags).")
    return params


def flag_check_params() -> Dict[str, Param]:
    """Extra param of DAGs that check source flags."""
    return {
        "skip_flag_check": Param(False, type="boolean", title="skip_flag_check",
                                 description="Run without waiting for the source flags (manual reloads only)."),
    }


# ── Dates ───────────────────────────────────────────────────────────────────────────────────────────────────
def default_cob_dt(dag_run: Any) -> date:
    """Run date in Vietnam time minus one day (T-1). run_after = when the run was due / triggered (UTC)."""
    run_after = pendulum.instance(dag_run.run_after)
    return run_after.in_timezone(LOCAL_TZ).date() - timedelta(days=1)


def _to_date(value: Any) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def resolve_dates(date_mode: str, params: Optional[Dict[str, Any]], dag_run: Any
                  ) -> Tuple[Optional[List[str]], List[date], str]:
    """(spark-submit date options or None = skip the job, dates to flag / check, human-readable reason)."""
    if date_mode == NO_DATE:
        return [], [], "job takes no date"
    params = params or {}
    from_dt, to_dt = params.get("from_dt"), params.get("to_dt")
    if from_dt or to_dt:
        if not (from_dt and to_dt):
            raise ValueError("from_dt and to_dt must be given together")
        start, end = _to_date(from_dt), _to_date(to_dt)
        if start > end:
            raise ValueError(f"from_dt {start} is after to_dt {end}")
        if date_mode == COB_DT_ONLY:
            return None, [], f"snapshot table: skipped for the range {start}..{end}"
        days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
        return ["--from-dt", start.isoformat(), "--to-dt", end.isoformat()], days, f"range {start}..{end}"
    if params.get("full_load") and date_mode == DATE_RANGE:
        return [], [], "full load (no date, no flags)"
    cob_dt = _to_date(params["cob_dt"]) if params.get("cob_dt") else default_cob_dt(dag_run)
    return ["--cob-dt", cob_dt.isoformat()], [cob_dt], f"cob_dt {cob_dt}"


def _run_info(ti: Any) -> Dict[str, object]:
    return {"dag_id": ti.dag_id, "run_id": ti.run_id, "task_id": ti.task_id, "try_number": ti.try_number}


def _stream(command: List[str]) -> int:
    """Run the command, copy its output line by line into the task log, return its exit code."""
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                          encoding="utf-8", errors="replace", bufsize=1) as proc:
        for line in proc.stdout:
            log.info("%s", line.rstrip())
        return proc.wait()


# ── Tasks ───────────────────────────────────────────────────────────────────────────────────────────────────
def spark_job_task(task_id: str, job_path: str, date_mode: str, flag_table: str,
                   trigger_rule: str = "all_success", extra_args: Optional[Sequence[str]] = None):
    """Task that runs one LakeHouse_Banking job with docker exec + spark-submit and writes its flags.

    Args:
        task_id:      task name shown in the UI, e.g. "ingest_customer".
        job_path:     full path of the job inside spark-client-dungafk (common.settings.job_path(...)).
        date_mode:    COB_DT_ONLY, DATE_RANGE or NO_DATE (which date options the job accepts).
        flag_table:   table the job writes, "<layer>.<schema>.<table>" — the flags are written for it.
        trigger_rule: "all_done" in a bronze domain DAG = run even when the table before failed.
        extra_args:   optional arguments placed right after job_path (e.g. ["--job", "silver__base__dim_party"]).
    """
    fixed_args = list(extra_args) if extra_args else []

    @task(task_id=task_id, pool=SPARK_POOL, trigger_rule=trigger_rule)
    def run_spark_job(params=None, dag_run=None, ti=None) -> str:
        date_options, dates, reason = resolve_dates(date_mode, params, dag_run)
        if date_options is None:
            raise AirflowSkipException(reason)
        command = ["docker", "exec", SPARK_CLIENT_CONTAINER, "spark-submit", job_path] + fixed_args + date_options
        log.info("Dates: %s | flags of %s: %s", reason, flag_table, [d.isoformat() for d in dates] or "none")
        log.info("Command: %s", " ".join(shlex.quote(part) for part in command))
        run_info = _run_info(ti)
        flags.set_flags(flag_table, dates, "RUNNING", run_info)
        exit_code = _stream(command)
        if exit_code != 0:
            flags.set_flags(flag_table, dates, "FAILED", run_info, f"spark-submit exit code {exit_code}")
            raise RuntimeError(f"{job_path} failed: spark-submit exit code {exit_code}")
        flags.set_flags(flag_table, dates, "SUCCESS", run_info)
        return f"{flag_table}: SUCCESS ({reason})"

    return run_spark_job()


def check_flags_task(source_tables: Sequence[str], date_mode: str, task_id: str = "check_source_flags"):
    """Sensor: done when every source table has a SUCCESS flag for every date of this run.

    Re-checks every FLAG_POKE_INTERVAL (reschedule mode: frees its slot between checks) and fails after
    FLAG_WAIT_TIMEOUT. Param skip_flag_check = true passes at once; a run with no single date (full load) has
    nothing to check and passes too.
    """
    source_tables = list(source_tables)

    @task.sensor(task_id=task_id, poke_interval=FLAG_POKE_INTERVAL.total_seconds(),
                 timeout=FLAG_WAIT_TIMEOUT.total_seconds(), mode="reschedule")
    def check_source_flags(params=None, dag_run=None) -> PokeReturnValue:
        params = params or {}
        if params.get("skip_flag_check"):
            log.warning("skip_flag_check = true: running without checking %s", source_tables)
            return PokeReturnValue(is_done=True)
        _, dates, reason = resolve_dates(date_mode, params, dag_run)
        if not dates:
            log.info("No date to check (%s): nothing to wait for.", reason)
            return PokeReturnValue(is_done=True)
        missing = flags.not_ready(source_tables, dates)
        if missing:
            for spec, cob_dt, status in missing:
                table, mode = flags.parse_table_spec(spec)
                if mode == "latest":
                    log.info("Waiting: %s (latest ≤ %s) has no SUCCESS flag", table, cob_dt)
                else:
                    log.info("Waiting: %s cob_dt %s is %s", table, cob_dt, status or "not loaded (no flag)")
            return PokeReturnValue(is_done=False)
        log.info("All sources ready (%s): %s", reason, source_tables)
        return PokeReturnValue(is_done=True)

    return check_source_flags()


def domain_done_task(domain: str):
    """Last task of a bronze domain DAG. trigger_rule none_failed: runs when no table failed (skipped tables are
    fine); when a table failed it becomes upstream_failed, so the DAG run is marked failed."""

    @task(task_id="domain_done", trigger_rule="none_failed")
    def domain_done() -> str:
        log.info("All tables of domain %s finished without failure.", domain)
        return domain

    return domain_done()
