"""
Settings shared by every DAG of Airflow_Banking.

Values that depend on the machine come from environment variables (.env, passed to every Airflow container by
docker-compose.yml); the rest are project rules kept in one place.
"""
import os
from datetime import timedelta

import pendulum

# Time zone of schedules and of the default cob_dt (the bank works in Vietnam time, not UTC)
TIMEZONE = "Asia/Ho_Chi_Minh"
LOCAL_TZ = pendulum.timezone(TIMEZONE)

# First date a DAG may be scheduled for. With catchup=False only the latest interval runs when a DAG is switched on.
START_DATE = pendulum.datetime(2026, 11, 1, tz=LOCAL_TZ)

# LakeHouse_Banking: the container that runs spark-submit and where its jobs/ folder is mounted inside it
SPARK_CLIENT_CONTAINER = os.environ.get("SPARK_CLIENT_CONTAINER", "spark-client-dungafk")
LAKEHOUSE_JOBS_ROOT = os.environ.get("LAKEHOUSE_JOBS_ROOT", "/home/spark/jobs")

# Airflow pool limiting how many Spark jobs run at the same time (created by airflow-init-dungafk)
SPARK_POOL = os.environ.get("SPARK_POOL", "spark_jobs")

# Flag store (common/flags.py): database ETL_CONTROL_DB_NAME on the Airflow metadata Postgres, same login
CONTROL_DB = {
    "host": os.environ.get("ETL_CONTROL_DB_HOST", "airflow-postgres-dungafk"),
    "port": int(os.environ.get("ETL_CONTROL_DB_PORT", "5432")),
    "dbname": os.environ.get("ETL_CONTROL_DB_NAME", "etl_control"),
    "user": os.environ.get("AIRFLOW_DB_USER"),
    "password": os.environ.get("AIRFLOW_DB_PASSWORD"),
}
# Database used only to CREATE the control database on first start (the Airflow metadata DB)
BOOTSTRAP_DB_NAME = os.environ.get("AIRFLOW_DB_NAME", "postgres")

# How a silver DAG waits for the flags of its sources (sensor in reschedule mode: no worker slot held while waiting)
FLAG_POKE_INTERVAL = timedelta(minutes=1)
FLAG_WAIT_TIMEOUT = timedelta(hours=4)

# Default arguments of every task
DEFAULT_ARGS = {
    "owner": "lakehouse",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def job_path(relative_path: str) -> str:
    """Full path, inside spark-client-dungafk, of a LakeHouse_Banking job, e.g. 'bronze/crm/bronze__crm__customer.py'."""
    return f"{LAKEHOUSE_JOBS_ROOT}/{relative_path}"
