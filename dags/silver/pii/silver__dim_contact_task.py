"""
Task of DAG silver__dim_contact: run the LakeHouse_Banking job jobs/silver/pii/silver__dim_contact.py
(Silver SCD3 (PII): bronze.crm.customer → silver.pii.dim_contact).

Imported by silver__dim_contact_dag.py — not a DAG file (skipped by dags/.airflowignore).
"""
from common.settings import job_path
from common.spark_job import DATE_RANGE, spark_job_task

JOB_PATH = job_path("silver/pii/silver__dim_contact.py")
DATE_MODE = DATE_RANGE


def load_dim_contact():
    """docker exec spark-client-dungafk spark-submit silver__dim_contact.py <date options>."""
    return spark_job_task(task_id="load_dim_contact", job_path=JOB_PATH, date_mode=DATE_MODE)
