"""
DAG silver__dim_date — when and in which order the tasks of table silver.base.dim_date run.

Silver: generated calendar → silver.base.dim_date (whole table rebuilt)
Job: LakeHouse_Banking jobs/silver/base/silver__dim_date.py (task file: silver__dim_date_task.py)

Tasks today: load_dim_date.
Planned later: check source flag → load_dim_date → save flag.
"""
from airflow.sdk import DAG

from silver.base.silver__dim_date_task import DATE_MODE, load_dim_date
from common.settings import DEFAULT_ARGS, START_DATE
from common.spark_job import date_params

with DAG(
    dag_id="silver__dim_date",
    description="Silver: generated calendar → silver.base.dim_date (whole table rebuilt)",
    schedule=None,               # manual only: run once, or after changing the date range in the job
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=date_params(DATE_MODE),
    tags=["silver", "base"],
    doc_md=__doc__,
):
    load_dim_date()
