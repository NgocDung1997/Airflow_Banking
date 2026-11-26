"""
DAG bronze__transaction — when and in which order the tasks of table bronze.core_banking.transaction run.

Bronze incremental: postgres core_banking.transaction → Iceberg bronze.core_banking.transaction
Job: LakeHouse_Banking jobs/bronze/core_banking/bronze__transaction.py (task file: bronze__transaction_task.py)

Tasks today: ingest_transaction.
Planned later: check source flag → ingest_transaction → save flag.
"""
from airflow.sdk import DAG

from bronze.core_banking.bronze__transaction_task import DATE_MODE, ingest_transaction
from common.settings import DEFAULT_ARGS, START_DATE
from common.spark_job import date_params

with DAG(
    dag_id="bronze__transaction",
    description="Bronze incremental: postgres core_banking.transaction → Iceberg bronze.core_banking.transaction",
    schedule="0 1 * * *",     # every day 01:00 Vietnam time (bronze)
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=date_params(DATE_MODE),
    tags=["bronze", "core_banking"],
    doc_md=__doc__,
):
    ingest_transaction()
