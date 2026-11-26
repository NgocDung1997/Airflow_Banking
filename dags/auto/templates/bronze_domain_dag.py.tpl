$header
"""
DAG $dag_id — every active bronze table of domain $domain, one after another:
    $order_text → domain_done

- A failed table does NOT stop the next ones (trigger_rule all_done). The last task domain_done then becomes
  upstream_failed, so the run is marked failed and the failed table is easy to spot (clear it to re-run it alone).
- Each table writes its flags (database etl_control, table job_flag); silver DAGs wait for them.
- Dates: see dags/common/spark_job.py. Snapshot tables are skipped for a from_dt / to_dt range.
"""
from airflow.sdk import DAG

$imports
from common.settings import DEFAULT_ARGS, START_DATE
from common.spark_job import DATE_RANGE, date_params, domain_done_task

with DAG(
    dag_id="$dag_id",
    description="Bronze: all tables of domain $domain ($table_count), one after another",
    schedule=$schedule,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=date_params(DATE_RANGE),
    tags=["bronze", "$domain", "generated"],
    doc_md=__doc__,
):
    tables = [
$task_calls
    ]
    for previous, current in zip(tables, tables[1:]):
        previous >> current
    tables >> domain_done_task("$domain")
