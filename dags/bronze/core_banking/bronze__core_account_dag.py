"""
DAG bronze__core_account — when and in which order the tasks of table bronze.core_banking.core_account run.

Bronze snapshot: postgres core_banking.core_account → Iceberg bronze.core_banking.core_account
Job: LakeHouse_Banking jobs/bronze/core_banking/bronze__core_account.py (task file: bronze__core_account_task.py)

Tasks today: ingest_core_account.
Planned later: check source flag → ingest_core_account → save flag.
"""
from airflow.sdk import DAG

from bronze.core_banking.bronze__core_account_task import DATE_MODE, ingest_core_account
from common.settings import DEFAULT_ARGS, START_DATE
from common.spark_job import date_params

with DAG(
    dag_id="bronze__core_account",
    description="Bronze snapshot: postgres core_banking.core_account → Iceberg bronze.core_banking.core_account",
    schedule="0 1 * * *",     # every day 01:00 Vietnam time (bronze)
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=date_params(DATE_MODE),
    tags=["bronze", "core_banking"],
    doc_md=__doc__,
):
    ingest_core_account()
