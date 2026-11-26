# How to run — Airflow_Banking

All commands from PowerShell in `E:\Study\LakeHouseProject\Airflow_Banking`.
The LakeHouse_Banking stack (`spark-client-dungafk` and the rest) must be up for tasks to run.

## 1. First start

```powershell
Copy-Item .env.example .env      # then fill in the change-me values (see the comments in the file)
docker compose build
docker compose up -d
docker compose ps                # all airflow-* "healthy"; airflow-init-dungafk "Exited (0)" is normal
```

`airflow-init-dungafk` runs on every `up`: migrates the metadata DB, creates the UI admin user, the pool
`spark_jobs`, and the flag database `etl_control` with its tables (all idempotent).

UI: http://localhost:8090 — user / password = `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` in `.env`.

## 2. Daily use

| What | How |
|---|---|
| Switch a DAG on / off | UI → DAGs → toggle (all DAGs start paused) |
| Run a bronze domain now (T-1) | UI → `bronze__<domain>` → Trigger (leave params empty) |
| Run one date | Trigger → `cob_dt` = `2026-11-23` |
| Backfill a range | Trigger → `from_dt` + `to_dt` (snapshot tables are skipped) |
| Full load | Trigger → `full_load` = true (no flags written) |
| Silver without waiting for flags | Trigger → `skip_flag_check` = true (manual reloads only) |
| Re-run only the failed table of a domain run | UI → DAG run → failed task → Clear (downstream too) |
| See the Spark output | UI → DAG run → task → Logs |
| Why is a silver DAG waiting? | Log of `check_source_flags`: "Waiting: bronze.x.y cob_dt ... is not loaded (no flag)" |

From the command line (runs the DAG once in the scheduler container, even when paused):

```powershell
docker exec airflow-scheduler-dungafk airflow dags test bronze__crm -c '{\"cob_dt\": \"2026-11-23\"}'
```

```powershell
docker exec airflow-scheduler-dungafk airflow dags list-import-errors
```

## 3. Flags

```powershell
docker exec airflow-postgres-dungafk psql -U airflow_dungnn -d etl_control -c "SELECT layer, schema_name, table_name, cob_dt, status, finished_at, message FROM job_flag ORDER BY cob_dt DESC, layer, table_name"
```

- History of every change: table `job_flag_history`.
- To force a re-check, fix the source (re-run the bronze table) — do not edit flags by hand unless you know why.

## 4. Add / change a table (generator)

1. Bronze: add the table to `LakeHouse_Banking/jobs/auto/bronze_tables.csv` and generate its LakeHouse job first
   (see LakeHouse_Banking/how_to_run.md). A new domain also needs a line in `dags/auto/config/bronze_domains.csv`.
2. Silver: create the LakeHouse job `jobs/silver/<ns>/silver__<table>.py`, then add a line to
   `dags/auto/config/silver_tables.csv` (`source_tables` = flags to wait for, `|`-separated).
3. Generate:

```powershell
python dags/auto/generate_dags.py --check
```

```powershell
python dags/auto/generate_dags.py
```

4. Wait ~30 s; `airflow dags list-import-errors` must show nothing. `active = 0` removes the generated files.
   A DAG whose file was removed stays in the UI with its history; delete it with
   `docker exec airflow-scheduler-dungafk airflow dags delete <dag_id> -y` when no longer needed.

## 5. Stack changes

- After changing `.env` or `docker-compose.yml`: `docker compose up -d --force-recreate`.
- After changing the Dockerfile: `docker compose build` then `docker compose up -d`.
- DAG files are mounted: changes are picked up by the dag-processor without restart.
- `AIRFLOW_DB_*` are read only when the volume `airflow_postgres_data` is empty (first start).
- `docker compose down -v` deletes the metadata DB **and the flags** — ask first.

## 6. Stop

```powershell
docker compose down
```
