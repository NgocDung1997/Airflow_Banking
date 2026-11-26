$header
"""
Task of DAG $dag_id: $func runs the LakeHouse_Banking job jobs/$job_rel
($description) and writes the flags of $flag_table.$sources_doc
"""
from common.settings import job_path
from common.spark_job import $date_mode, spark_job_task

JOB_PATH = job_path("$job_rel")
DATE_MODE = $date_mode
FLAG_TABLE = "$flag_table"$sources_const


def $func(trigger_rule: str = "all_success"):
    """docker exec spark-client-dungafk spark-submit $job_file <date options> + flags of $flag_table."""
    return spark_job_task(task_id="$func", job_path=JOB_PATH, date_mode=DATE_MODE, flag_table=FLAG_TABLE,
                          trigger_rule=trigger_rule)
