# Known issues — Airflow_Banking

Errors and problems already met, and how they were fixed. Format per entry: **You see / Why / Fix**.
Look here first when something breaks. Problems inside the Spark jobs belong to `LakeHouse_Banking/known_issues/`.

## Index

| ID | Category | Title |
|---|---|---|
| [KI-001](#ki-001-spark-submit-exit-code-2-no-such-file-or-directory-after-lakehouse_banking-refactor) | DAGs & tasks | `spark-submit exit code 2` (`No such file or directory`) after LakeHouse_Banking refactor |

## Setup & configuration

## DAGs & tasks

### KI-001: `spark-submit exit code 2` (`No such file or directory`) after LakeHouse_Banking refactor
- **You see:** A DAG task fails in ~1–2 s with `spark-submit exit code 2` and `python3: can't open file '/home/spark/jobs/...': [Errno 2] No such file or directory` (and retries after 5 min).
- **Why:** Job paths, domain names (`core_banking` → `t24`), or runner conventions in `LakeHouse_Banking` changed (`bronze__<domain>__<table>.py`, YAML silver jobs run via `silver/run_silver.py --job silver__<ns>__<table>`), while `Airflow_Banking` configs and generated DAGs were still pointing to the old paths.
- **Fix:** Sync `dags/auto/config/bronze_domains.csv`, `dags/auto/config/silver_tables.csv`, and `dags/auto/generate_dags.py` with `LakeHouse_Banking/how_to_run.md`, re-run `python dags/auto/generate_dags.py`, and delete any obsolete DAG from Airflow metadata (`airflow dags delete <old_dag_id> -y`).
