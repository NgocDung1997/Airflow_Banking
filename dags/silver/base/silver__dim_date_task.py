"""
Task of DAG silver__dim_date: run the LakeHouse_Banking job jobs/silver/base/silver__dim_date.py
(Silver: generated calendar → silver.base.dim_date (whole table rebuilt)).

Imported by silver__dim_date_dag.py — not a DAG file (skipped by dags/.airflowignore).
"""
from common.settings import job_path
from common.spark_job import NO_DATE, spark_job_task

JOB_PATH = job_path("silver/base/silver__dim_date.py")
DATE_MODE = NO_DATE


def load_dim_date():
    """docker exec spark-client-dungafk spark-submit silver__dim_date.py <date options>."""
    return spark_job_task(task_id="load_dim_date", job_path=JOB_PATH, date_mode=DATE_MODE)
