"""
Task of DAG bronze__account: run the LakeHouse_Banking job jobs/bronze/core_banking/bronze__account.py
(Bronze snapshot: postgres core_banking.account → Iceberg bronze.core_banking.account).

Imported by bronze__account_dag.py — not a DAG file (skipped by dags/.airflowignore).
"""
from common.settings import job_path
from common.spark_job import COB_DT_ONLY, spark_job_task

JOB_PATH = job_path("bronze/core_banking/bronze__account.py")
DATE_MODE = COB_DT_ONLY


def ingest_account():
    """docker exec spark-client-dungafk spark-submit bronze__account.py <date options>."""
    return spark_job_task(task_id="ingest_account", job_path=JOB_PATH, date_mode=DATE_MODE)
