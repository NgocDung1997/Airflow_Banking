"""
DAG silver__dim_party_identity — when and in which order the tasks of table silver.pii.dim_party_identity run.

Silver SCD1 (PII): bronze.crm.customer → silver.pii.dim_party_identity
Job: LakeHouse_Banking jobs/silver/pii/silver__dim_party_identity.py (task file: silver__dim_party_identity_task.py)

Tasks today: load_dim_party_identity.
Planned later: check source flag → load_dim_party_identity → save flag.
"""
from airflow.sdk import DAG

from silver.pii.silver__dim_party_identity_task import DATE_MODE, load_dim_party_identity
from common.settings import DEFAULT_ARGS, START_DATE
from common.spark_job import date_params

with DAG(
    dag_id="silver__dim_party_identity",
    description="Silver SCD1 (PII): bronze.crm.customer → silver.pii.dim_party_identity",
    schedule="15 2 * * *",    # every day 02:15 Vietnam time (silver PII dims)
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=date_params(DATE_MODE),
    tags=["silver", "pii"],
    doc_md=__doc__,
):
    load_dim_party_identity()
