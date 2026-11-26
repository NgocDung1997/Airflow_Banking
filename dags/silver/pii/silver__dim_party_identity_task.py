"""
Task of DAG silver__dim_party_identity: run the LakeHouse_Banking job jobs/silver/pii/silver__dim_party_identity.py
(Silver SCD1 (PII): bronze.crm.customer → silver.pii.dim_party_identity).

Imported by silver__dim_party_identity_dag.py — not a DAG file (skipped by dags/.airflowignore).
"""
from common.settings import job_path
from common.spark_job import DATE_RANGE, spark_job_task

JOB_PATH = job_path("silver/pii/silver__dim_party_identity.py")
DATE_MODE = DATE_RANGE


def load_dim_party_identity():
    """docker exec spark-client-dungafk spark-submit silver__dim_party_identity.py <date options>."""
    return spark_job_task(task_id="load_dim_party_identity", job_path=JOB_PATH, date_mode=DATE_MODE)
