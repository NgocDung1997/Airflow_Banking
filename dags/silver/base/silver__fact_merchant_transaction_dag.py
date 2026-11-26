"""
DAG silver__fact_merchant_transaction — when and in which order the tasks of table silver.base.fact_merchant_transaction run.

Silver upsert: bronze.payment.merchant_transaction → silver.base.fact_merchant_transaction
Job: LakeHouse_Banking jobs/silver/base/silver__fact_merchant_transaction.py (task file: silver__fact_merchant_transaction_task.py)

Tasks today: load_fact_merchant_transaction.
Planned later: check source flag → load_fact_merchant_transaction → save flag.
"""
from airflow.sdk import DAG

from silver.base.silver__fact_merchant_transaction_task import DATE_MODE, load_fact_merchant_transaction
from common.settings import DEFAULT_ARGS, START_DATE
from common.spark_job import date_params

with DAG(
    dag_id="silver__fact_merchant_transaction",
    description="Silver upsert: bronze.payment.merchant_transaction → silver.base.fact_merchant_transaction",
    schedule="30 2 * * *",    # every day 02:30 Vietnam time (silver facts, after the dims)
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=date_params(DATE_MODE),
    tags=["silver", "base"],
    doc_md=__doc__,
):
    load_fact_merchant_transaction()
