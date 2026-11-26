"""
Task of DAG bronze__merchant_transaction: run the LakeHouse_Banking job jobs/bronze/payment/bronze__merchant_transaction.py
(Bronze incremental: postgres payment.merchant_transaction → Iceberg bronze.payment.merchant_transaction).

Imported by bronze__merchant_transaction_dag.py — not a DAG file (skipped by dags/.airflowignore).
"""
from common.settings import job_path
from common.spark_job import DATE_RANGE, spark_job_task

JOB_PATH = job_path("bronze/payment/bronze__merchant_transaction.py")
DATE_MODE = DATE_RANGE


def ingest_merchant_transaction():
    """docker exec spark-client-dungafk spark-submit bronze__merchant_transaction.py <date options>."""
    return spark_job_task(task_id="ingest_merchant_transaction", job_path=JOB_PATH, date_mode=DATE_MODE)
