"""
DAG silver__dim_account — when and in which order the tasks of table silver.base.dim_account run.

Silver SCD2: bronze.core_banking.account → silver.base.dim_account
Job: LakeHouse_Banking jobs/silver/base/silver__dim_account.py (task file: silver__dim_account_task.py)

Tasks today: load_dim_account.
Planned later: check source flag → load_dim_account → save flag.
"""
from airflow.sdk import DAG

from silver.base.silver__dim_account_task import DATE_MODE, load_dim_account
from common.settings import DEFAULT_ARGS, START_DATE
from common.spark_job import date_params

with DAG(
    dag_id="silver__dim_account",
    description="Silver SCD2: bronze.core_banking.account → silver.base.dim_account",
    schedule="0 2 * * *",     # every day 02:00 Vietnam time (silver dims, after bronze)
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=date_params(DATE_MODE),
    tags=["silver", "base"],
    doc_md=__doc__,
):
    load_dim_account()
