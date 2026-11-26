"""
Task of DAG silver__fact_transaction: run the LakeHouse_Banking job jobs/silver/base/silver__fact_transaction.py
(Silver upsert: bronze.core_banking.transaction → silver.base.fact_transaction).

Imported by silver__fact_transaction_dag.py — not a DAG file (skipped by dags/.airflowignore).
"""
from common.settings import job_path
from common.spark_job import DATE_RANGE, spark_job_task

JOB_PATH = job_path("silver/base/silver__fact_transaction.py")
DATE_MODE = DATE_RANGE


def load_fact_transaction():
    """docker exec spark-client-dungafk spark-submit silver__fact_transaction.py <date options>."""
    return spark_job_task(task_id="load_fact_transaction", job_path=JOB_PATH, date_mode=DATE_MODE)
