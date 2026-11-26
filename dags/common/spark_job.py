"""
Build the Airflow pieces that run ONE LakeHouse_Banking Spark job:

    docker exec spark-client-dungafk spark-submit /home/spark/jobs/<layer>/<domain>/<job>.py <date options>

The task is a Bash task: its exit code is the exit code of spark-submit, so a failed Spark job fails the task
(and Airflow retries it). The full spark-submit output goes to the Airflow task log.

Date options (the same as the LakeHouse jobs, see LakeHouse_Banking/how_to_run.md):
    COB_DT_ONLY  bronze snapshot jobs: --cob-dt only
    DATE_RANGE   bronze incremental + silver jobs: --cob-dt, or --from-dt/--to-dt, or nothing (= full load)
    NO_DATE      jobs without date options (silver__dim_date)

Which date is passed:
    1. params from_dt + to_dt (manual "Trigger DAG w/ config")    → --from-dt X --to-dt Y   (DATE_RANGE only)
    2. param full_load = true                                     → no date = FULL LOAD      (DATE_RANGE only)
    3. param cob_dt                                               → --cob-dt X
    4. nothing (scheduled run)                                    → --cob-dt = run date (Vietnam time) - 1 day
"""
import shlex
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import pendulum
from airflow.sdk import Param, task

from common.settings import LOCAL_TZ, SPARK_CLIENT_CONTAINER, SPARK_POOL

COB_DT_ONLY = "cob_dt_only"
DATE_RANGE = "date_range"
NO_DATE = "no_date"


def date_params(date_mode: str) -> Dict[str, Param]:
    """DAG params (form of "Trigger DAG w/ config") that match the date options of the job."""
    if date_mode == NO_DATE:
        return {}
    params = {
        "cob_dt": Param(None, type=["null", "string"], format="date", title="cob_dt",
                        description="Business date to load (YYYY-MM-DD). Empty = run date - 1 day."),
    }
    if date_mode == DATE_RANGE:
        params["from_dt"] = Param(None, type=["null", "string"], format="date", title="from_dt",
                                  description="Backfill start (YYYY-MM-DD), together with to_dt. Wins over cob_dt.")
        params["to_dt"] = Param(None, type=["null", "string"], format="date", title="to_dt",
                                description="Backfill end (YYYY-MM-DD), together with from_dt.")
        params["full_load"] = Param(False, type="boolean", title="full_load",
                                    description="Run the job with no date = FULL LOAD (ignores the dates above).")
    return params


def default_cob_dt(dag_run: Any) -> date:
    """Run date in Vietnam time minus one day (T-1). run_after = when the run was due / triggered (UTC)."""
    run_after = pendulum.instance(dag_run.run_after)
    return run_after.in_timezone(LOCAL_TZ).date() - timedelta(days=1)


def date_args(date_mode: str, params: Optional[Dict[str, Any]], dag_run: Any) -> List[str]:
    """Date options to append to spark-submit (see the module docstring for the order of precedence)."""
    if date_mode == NO_DATE:
        return []
    params = params or {}
    if date_mode == DATE_RANGE:
        from_dt, to_dt = params.get("from_dt"), params.get("to_dt")
        if from_dt or to_dt:
            if not (from_dt and to_dt):
                raise ValueError("from_dt and to_dt must be given together")
            return ["--from-dt", from_dt, "--to-dt", to_dt]
        if params.get("full_load"):
            return []
    cob_dt = params.get("cob_dt") or default_cob_dt(dag_run).isoformat()
    return ["--cob-dt", cob_dt]


def spark_job_task(task_id: str, job_path: str, date_mode: str):
    """Airflow task that runs one LakeHouse_Banking job with docker exec + spark-submit.

    Args:
        task_id:   task name shown in the UI, e.g. "ingest_customer".
        job_path:  full path of the job inside spark-client-dungafk (common.settings.job_path(...)).
        date_mode: COB_DT_ONLY, DATE_RANGE or NO_DATE (which date options the job accepts).
    """

    @task.bash(task_id=task_id, pool=SPARK_POOL)
    def run_spark_job(params=None, dag_run=None) -> str:
        command = ["docker", "exec", SPARK_CLIENT_CONTAINER, "spark-submit", job_path]
        command += date_args(date_mode, params, dag_run)
        return " ".join(shlex.quote(part) for part in command)

    return run_spark_job()
