$header
"""
DAG $dag_id — load table $flag_table.

$description
Job: LakeHouse_Banking jobs/$job_rel (task file: ${dag_id}_task.py)
Flow: $flow_text
$flow_doc
"""
from airflow.sdk import DAG

from $task_module import $task_imports
from common.settings import DEFAULT_ARGS, START_DATE
from common.spark_job import $spark_job_imports

with DAG(
    dag_id="$dag_id",
    description="$description",
    schedule=$schedule,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=$params,
    tags=["silver", "$namespace", "generated"],
    doc_md=__doc__,
):
    $body
