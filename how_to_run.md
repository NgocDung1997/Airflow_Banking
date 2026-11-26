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

`airflow-init-dungafk` runs once per `up`: migrates the metadata DB, creates the UI admin user and the pool `spark_jobs`.

UI: http://localhost:8090 — user / password = `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` in `.env`.

## 2. Daily use

| What | How |
|---|---|
| Switch a DAG on / off | UI → DAGs → toggle (all DAGs start paused) |
| Run now with T-1 | UI → DAG → Trigger (leave params empty) |
| Run one date | UI → DAG → Trigger → `cob_dt` = `2026-11-23` |
| Backfill a range | UI → DAG → Trigger → `from_dt` + `to_dt` (not for bronze snapshot DAGs) |
| Full load | UI → DAG → Trigger → `full_load` = true (not for bronze snapshot DAGs) |
| See the Spark output | UI → DAG run → task → Logs |

From the command line (runs the DAG once in the scheduler container, even when paused):

```powershell
docker exec airflow-scheduler-dungafk airflow dags test bronze__customer -c '{\"cob_dt\": \"2026-11-23\"}'
```

```powershell
docker exec airflow-scheduler-dungafk airflow dags list
```

```powershell
docker exec airflow-scheduler-dungafk airflow dags list-import-errors
```

## 3. Add a table

1. Make sure the LakeHouse job exists: `LakeHouse_Banking/jobs/<layer>/<domain>/<layer>__<table>.py`.
2. Copy the two files of a similar table in `dags/<layer>/<domain>/` and rename them `<layer>__<table>_dag.py` /
   `<layer>__<table>_task.py`.
3. In the task file: job path, `DATE_MODE` (`COB_DT_ONLY` / `DATE_RANGE` / `NO_DATE`), function and task name.
4. In the DAG file: the import, `dag_id`, description, schedule, tags.
5. Wait ~30 s, then `airflow dags list-import-errors` must show nothing.

## 4. Stack changes

- After changing `.env` or `docker-compose.yml`: `docker compose up -d --force-recreate`.
- After changing the Dockerfile: `docker compose build` then `docker compose up -d`.
- DAG files are mounted: changes are picked up by the dag-processor without restart.
- `AIRFLOW_DB_*` are read only when the volume `airflow_postgres_data` is empty (first start).
- `docker compose down -v` deletes the metadata DB (run history, users) — ask first.

## 5. Stop

```powershell
docker compose down
```
