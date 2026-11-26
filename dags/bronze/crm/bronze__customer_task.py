"""
Task of DAG bronze__customer: run the LakeHouse_Banking job jobs/bronze/crm/bronze__customer.py
(Bronze snapshot: postgres crm.customer → Iceberg bronze.crm.customer).

Imported by bronze__customer_dag.py — not a DAG file (skipped by dags/.airflowignore).
"""
from common.settings import job_path
from common.spark_job import COB_DT_ONLY, spark_job_task

JOB_PATH = job_path("bronze/crm/bronze__customer.py")
DATE_MODE = COB_DT_ONLY


def ingest_customer():
    """docker exec spark-client-dungafk spark-submit bronze__customer.py <date options>."""
    return spark_job_task(task_id="ingest_customer", job_path=JOB_PATH, date_mode=DATE_MODE)
