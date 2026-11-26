"""
DAG bronze__customer — when and in which order the tasks of table bronze.crm.customer run.

Bronze snapshot: postgres crm.customer → Iceberg bronze.crm.customer
Job: LakeHouse_Banking jobs/bronze/crm/bronze__customer.py (task file: bronze__customer_task.py)

Tasks today: ingest_customer.
Planned later: check source flag → ingest_customer → save flag.
"""
from airflow.sdk import DAG

from bronze.crm.bronze__customer_task import DATE_MODE, ingest_customer
from common.settings import DEFAULT_ARGS, START_DATE
from common.spark_job import date_params

with DAG(
    dag_id="bronze__customer",
    description="Bronze snapshot: postgres crm.customer → Iceberg bronze.crm.customer",
    schedule="0 1 * * *",     # every day 01:00 Vietnam time (bronze)
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=date_params(DATE_MODE),
    tags=["bronze", "crm"],
    doc_md=__doc__,
):
    ingest_customer()
