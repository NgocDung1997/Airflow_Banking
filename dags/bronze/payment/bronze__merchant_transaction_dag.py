"""
DAG bronze__merchant_transaction — when and in which order the tasks of table bronze.payment.merchant_transaction run.

Bronze incremental: postgres payment.merchant_transaction → Iceberg bronze.payment.merchant_transaction
Job: LakeHouse_Banking jobs/bronze/payment/bronze__merchant_transaction.py (task file: bronze__merchant_transaction_task.py)

Tasks today: ingest_merchant_transaction.
Planned later: check source flag → ingest_merchant_transaction → save flag.
"""
from airflow.sdk import DAG

from bronze.payment.bronze__merchant_transaction_task import DATE_MODE, ingest_merchant_transaction
from common.settings import DEFAULT_ARGS, START_DATE
from common.spark_job import date_params

with DAG(
    dag_id="bronze__merchant_transaction",
    description="Bronze incremental: postgres payment.merchant_transaction → Iceberg bronze.payment.merchant_transaction",
    schedule="0 1 * * *",     # every day 01:00 Vietnam time (bronze)
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=date_params(DATE_MODE),
    tags=["bronze", "payment"],
    doc_md=__doc__,
):
    ingest_merchant_transaction()
