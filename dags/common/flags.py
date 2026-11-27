"""
Job flags: one row per (table, cob_dt) telling whether that table was loaded for that business date.

Where: database `etl_control` (ETL_CONTROL_DB_NAME) on airflow-postgres-dungafk, same login as the Airflow metadata DB.

    job_flag          current state, primary key (layer, schema_name, table_name, cob_dt)
    job_flag_history  every state change, append-only (who / when / which try)

Status:
    RUNNING  the Spark job started for this date
    SUCCESS  the Spark job ended with exit code 0  → tables downstream may run
    FAILED   the Spark job failed (message = reason)

Writers: every Spark task (common/spark_job.py) sets RUNNING → SUCCESS / FAILED for its own table and dates.
Readers: the check_source_flags sensor of a silver DAG waits until all its source tables are SUCCESS for its dates.
A table is named "<layer>.<schema>.<table>", e.g. "bronze.crm.customer", "silver.base.dim_party".

Create the database + tables (idempotent; airflow-init-dungafk runs it on every start):
    python -m common.flags          (from /opt/airflow/dags)
"""
from datetime import date
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2

from common.settings import BOOTSTRAP_DB_NAME, CONTROL_DB

STATUSES = ("RUNNING", "SUCCESS", "FAILED")

DDL = """
CREATE TABLE IF NOT EXISTS job_flag (
    layer        TEXT        NOT NULL,              -- bronze / silver / gold
    schema_name  TEXT        NOT NULL,              -- bronze: domain (crm ...); silver: namespace (base, pii)
    table_name   TEXT        NOT NULL,
    cob_dt       DATE        NOT NULL,              -- business date the flag is for
    status       TEXT        NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    dag_id       TEXT,
    run_id       TEXT,
    task_id      TEXT,
    try_number   INT,
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    message      TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (layer, schema_name, table_name, cob_dt)
);
CREATE TABLE IF NOT EXISTS job_flag_history (
    LIKE job_flag INCLUDING DEFAULTS,
    logged_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS job_flag_history_table_idx
    ON job_flag_history (layer, schema_name, table_name, cob_dt);
"""

UPSERT = """
INSERT INTO job_flag (layer, schema_name, table_name, cob_dt, status, dag_id, run_id, task_id, try_number,
                      started_at, finished_at, message, updated_at)
VALUES (%(layer)s, %(schema)s, %(table)s, %(cob_dt)s, %(status)s, %(dag_id)s, %(run_id)s, %(task_id)s,
        %(try_number)s,
        CASE WHEN %(status)s = 'RUNNING' THEN now() END,
        CASE WHEN %(status)s <> 'RUNNING' THEN now() END,
        %(message)s, now())
ON CONFLICT (layer, schema_name, table_name, cob_dt) DO UPDATE SET
    status      = EXCLUDED.status,
    dag_id      = EXCLUDED.dag_id,
    run_id      = EXCLUDED.run_id,
    task_id     = EXCLUDED.task_id,
    try_number  = EXCLUDED.try_number,
    -- a new RUNNING restarts the clock; SUCCESS / FAILED keep the start time of the same run
    started_at  = CASE WHEN EXCLUDED.status = 'RUNNING' THEN now() ELSE job_flag.started_at END,
    finished_at = CASE WHEN EXCLUDED.status = 'RUNNING' THEN NULL ELSE now() END,
    message     = EXCLUDED.message,
    updated_at  = now()
RETURNING *
"""

HISTORY = """
INSERT INTO job_flag_history (layer, schema_name, table_name, cob_dt, status, dag_id, run_id, task_id, try_number,
                              started_at, finished_at, message, updated_at)
VALUES (%(layer)s, %(schema_name)s, %(table_name)s, %(cob_dt)s, %(status)s, %(dag_id)s, %(run_id)s, %(task_id)s,
        %(try_number)s, %(started_at)s, %(finished_at)s, %(message)s, %(updated_at)s)
"""


def split_table(full_name: str) -> Tuple[str, str, str]:
    """'bronze.crm.customer' → ('bronze', 'crm', 'customer')."""
    parts = full_name.split(".")
    if len(parts) != 3 or not all(parts):
        raise ValueError(f"table must be '<layer>.<schema>.<table>', got {full_name!r}")
    return parts[0], parts[1], parts[2]


def connect(dbname: Optional[str] = None):
    """psycopg2 connection to the control database (or to `dbname`)."""
    params = dict(CONTROL_DB)
    if dbname:
        params["dbname"] = dbname
    return psycopg2.connect(**params)


def set_flags(table: str, dates: Iterable[date], status: str, run_info: Dict[str, object],
              message: Optional[str] = None) -> int:
    """Write `status` for `table` on every date in `dates` (one transaction). Returns the number of flags written.

    run_info: dag_id, run_id, task_id, try_number of the Airflow task writing the flag.
    """
    if status not in STATUSES:
        raise ValueError(f"status must be one of {STATUSES}, got {status!r}")
    layer, schema, name = split_table(table)
    dates = list(dates)
    if not dates:
        return 0
    with connect() as conn, conn.cursor() as cur:
        columns = None
        for cob_dt in dates:
            cur.execute(UPSERT, {"layer": layer, "schema": schema, "table": name, "cob_dt": cob_dt,
                                 "status": status, "message": message,
                                 "dag_id": run_info.get("dag_id"), "run_id": run_info.get("run_id"),
                                 "task_id": run_info.get("task_id"), "try_number": run_info.get("try_number")})
            columns = columns or [c.name for c in cur.description]
            cur.execute(HISTORY, dict(zip(columns, cur.fetchone())))
    conn.close()
    return len(dates)


CHECK_MODES = ("exact", "latest")


def parse_table_spec(spec: str) -> Tuple[str, str]:
    """Parse '<layer>.<schema>.<table>[@exact|@latest]' → (table_name, mode). No suffix = 'exact'."""
    if "@" in spec:
        table, mode = spec.rsplit("@", 1)
        if mode not in CHECK_MODES:
            raise ValueError(f"unsupported flag check mode {mode!r} in {spec!r}; use {' or '.join(CHECK_MODES)}")
        return table, mode
    return spec, "exact"


def not_ready(tables: Sequence[str], dates: Iterable[date]) -> List[Tuple[str, date, Optional[str]]]:
    """Return (table_spec, cob_dt, status or None) for every parent / date that is not ready. Empty list = all ready.

    Each entry in `tables` may carry a mode suffix:
        <table_name> or <table_name>@exact   ready for date D when status = 'SUCCESS' on cob_dt = D
        <table_name>@latest                  ready for date D when a status = 'SUCCESS' row exists on cob_dt <= D
                                             (missing rows return status None)
    Runs one query per table (never one query per date).
    """
    dates = list(dates)
    specs = [(spec, *parse_table_spec(spec)) for spec in tables]   # validate all specs upfront, even if dates is empty
    if not specs or not dates:
        return []
    missing: List[Tuple[str, date, Optional[str]]] = []
    with connect() as conn, conn.cursor() as cur:
        for spec, table, mode in specs:
            layer, schema, name = split_table(table)
            if mode == "exact":
                cur.execute("SELECT cob_dt, status FROM job_flag "
                            "WHERE layer = %s AND schema_name = %s AND table_name = %s AND cob_dt = ANY(%s)",
                            (layer, schema, name, dates))
                found = dict(cur.fetchall())
                for d in dates:
                    if found.get(d) != "SUCCESS":
                        missing.append((spec, d, found.get(d)))
            else:
                # EXISTS (... AND status = 'SUCCESS' AND cob_dt <= D)  <=>  MIN(cob_dt of SUCCESS) <= D
                cur.execute("SELECT MIN(cob_dt) FROM job_flag "
                            "WHERE layer = %s AND schema_name = %s AND table_name = %s AND status = 'SUCCESS'",
                            (layer, schema, name))
                earliest = cur.fetchone()[0]
                for d in dates:
                    if earliest is None or earliest > d:
                        missing.append((spec, d, None))
    conn.close()
    return missing


def init_control_db() -> None:
    """Create the control database (if missing) and its tables (if missing)."""
    conn = connect(BOOTSTRAP_DB_NAME)
    conn.autocommit = True          # CREATE DATABASE cannot run inside a transaction
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (CONTROL_DB["dbname"],))
        if cur.fetchone() is None:
            cur.execute(f'CREATE DATABASE "{CONTROL_DB["dbname"]}"')
            print(f"Database {CONTROL_DB['dbname']} created.")
    conn.close()
    with connect() as conn, conn.cursor() as cur:
        cur.execute(DDL)
    conn.close()
    print(f"Flag tables ready in database {CONTROL_DB['dbname']}.")


if __name__ == "__main__":
    init_control_db()
