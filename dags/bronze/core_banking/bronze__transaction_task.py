"""
Task of DAG bronze__transaction: run the LakeHouse_Banking job jobs/bronze/core_banking/bronze__transaction.py
(Bronze incremental: postgres core_banking.transaction → Iceberg bronze.core_banking.transaction).

Imported by bronze__transaction_dag.py — not a DAG file (skipped by dags/.airflowignore).
"""
from common.settings import job_path
from common.spark_job import DATE_RANGE, spark_job_task

JOB_PATH = job_path("bronze/core_banking/bronze__transaction.py")
DATE_MODE = DATE_RANGE


def ingest_transaction():
    """docker exec spark-client-dungafk spark-submit bronze__transaction.py <date options>."""
    return spark_job_task(task_id="ingest_transaction", job_path=JOB_PATH, date_mode=DATE_MODE)
