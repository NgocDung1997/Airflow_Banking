"""
DAG bronze__account — when and in which order the tasks of table bronze.core_banking.account run.

Bronze snapshot: postgres core_banking.account → Iceberg bronze.core_banking.account
Job: LakeHouse_Banking jobs/bronze/core_banking/bronze__account.py (task file: bronze__account_task.py)

Tasks today: ingest_account.
Planned later: check source flag → ingest_account → save flag.
"""
from airflow.sdk import DAG

from bronze.core_banking.bronze__account_task import DATE_MODE, ingest_account
from common.settings import DEFAULT_ARGS, START_DATE
from common.spark_job import date_params

with DAG(
    dag_id="bronze__account",
    description="Bronze snapshot: postgres core_banking.account → Iceberg bronze.core_banking.account",
    schedule="0 1 * * *",     # every day 01:00 Vietnam time (bronze)
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=date_params(DATE_MODE),
    tags=["bronze", "core_banking"],
    doc_md=__doc__,
):
    ingest_account()
