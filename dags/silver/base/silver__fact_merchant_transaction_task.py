"""
Task of DAG silver__fact_merchant_transaction: run the LakeHouse_Banking job jobs/silver/base/silver__fact_merchant_transaction.py
(Silver upsert: bronze.payment.merchant_transaction → silver.base.fact_merchant_transaction).

Imported by silver__fact_merchant_transaction_dag.py — not a DAG file (skipped by dags/.airflowignore).
"""
from common.settings import job_path
from common.spark_job import DATE_RANGE, spark_job_task

JOB_PATH = job_path("silver/base/silver__fact_merchant_transaction.py")
DATE_MODE = DATE_RANGE


def load_fact_merchant_transaction():
    """docker exec spark-client-dungafk spark-submit silver__fact_merchant_transaction.py <date options>."""
    return spark_job_task(task_id="load_fact_merchant_transaction", job_path=JOB_PATH, date_mode=DATE_MODE)
