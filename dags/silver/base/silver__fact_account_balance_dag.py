"""
DAG silver__fact_account_balance — when and in which order the tasks of table silver.base.fact_account_balance run.

Silver: bronze.core_banking.account → silver.base.fact_account_balance
Job: LakeHouse_Banking jobs/silver/base/silver__fact_account_balance.py (task file: silver__fact_account_balance_task.py)

Tasks today: load_fact_account_balance.
Planned later: check source flag → load_fact_account_balance → save flag.
"""
from airflow.sdk import DAG

from silver.base.silver__fact_account_balance_task import DATE_MODE, load_fact_account_balance
from common.settings import DEFAULT_ARGS, START_DATE
from common.spark_job import date_params

with DAG(
    dag_id="silver__fact_account_balance",
    description="Silver: bronze.core_banking.account → silver.base.fact_account_balance",
    schedule="30 2 * * *",    # every day 02:30 Vietnam time (silver facts, after the dims)
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=date_params(DATE_MODE),
    tags=["silver", "base"],
    doc_md=__doc__,
):
    load_fact_account_balance()
