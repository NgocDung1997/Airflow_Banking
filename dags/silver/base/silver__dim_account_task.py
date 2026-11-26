"""
Task of DAG silver__dim_account: run the LakeHouse_Banking job jobs/silver/base/silver__dim_account.py
(Silver SCD2: bronze.core_banking.account → silver.base.dim_account).

Imported by silver__dim_account_dag.py — not a DAG file (skipped by dags/.airflowignore).
"""
from common.settings import job_path
from common.spark_job import DATE_RANGE, spark_job_task

JOB_PATH = job_path("silver/base/silver__dim_account.py")
DATE_MODE = DATE_RANGE


def load_dim_account():
    """docker exec spark-client-dungafk spark-submit silver__dim_account.py <date options>."""
    return spark_job_task(task_id="load_dim_account", job_path=JOB_PATH, date_mode=DATE_MODE)
