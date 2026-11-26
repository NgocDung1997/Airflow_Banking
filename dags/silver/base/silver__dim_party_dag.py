"""
DAG silver__dim_party — when and in which order the tasks of table silver.base.dim_party run.

Silver SCD2: bronze.crm.customer → silver.base.dim_party
Job: LakeHouse_Banking jobs/silver/base/silver__dim_party.py (task file: silver__dim_party_task.py)

Tasks today: load_dim_party.
Planned later: check source flag → load_dim_party → save flag.
"""
from airflow.sdk import DAG

from silver.base.silver__dim_party_task import DATE_MODE, load_dim_party
from common.settings import DEFAULT_ARGS, START_DATE
from common.spark_job import date_params

with DAG(
    dag_id="silver__dim_party",
    description="Silver SCD2: bronze.crm.customer → silver.base.dim_party",
    schedule="0 2 * * *",     # every day 02:00 Vietnam time (silver dims, after bronze)
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=date_params(DATE_MODE),
    tags=["silver", "base"],
    doc_md=__doc__,
):
    load_dim_party()
