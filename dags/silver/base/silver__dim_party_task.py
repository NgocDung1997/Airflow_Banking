"""
Task of DAG silver__dim_party: run the LakeHouse_Banking job jobs/silver/base/silver__dim_party.py
(Silver SCD2: bronze.crm.customer → silver.base.dim_party).

Imported by silver__dim_party_dag.py — not a DAG file (skipped by dags/.airflowignore).
"""
from common.settings import job_path
from common.spark_job import DATE_RANGE, spark_job_task

JOB_PATH = job_path("silver/base/silver__dim_party.py")
DATE_MODE = DATE_RANGE


def load_dim_party():
    """docker exec spark-client-dungafk spark-submit silver__dim_party.py <date options>."""
    return spark_job_task(task_id="load_dim_party", job_path=JOB_PATH, date_mode=DATE_MODE)
