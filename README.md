# Airflow_Banking

Airflow for jobs in the Banking domain: Airflow 3 (LocalExecutor) in Docker that schedules the Spark jobs of
[LakeHouse_Banking](../LakeHouse_Banking) (bronze and silver layers).

Commands: [how_to_run.md](how_to_run.md) · Problems already met: [known_issues/README.md](known_issues/README.md)

## Architecture

```
 Airflow_Banking (network airflow-net-dungafk)            LakeHouse_Banking (network lakehouse-net-dungafk)
 ┌──────────────────────────────────────────┐            ┌───────────────────────────────────────────────┐
 │ airflow-apiserver-dungafk   UI :8090     │            │ spark-client-dungafk  (driver, client mode)   │
 │ airflow-scheduler-dungafk   runs tasks ──┼── docker ──┼─▶ spark-submit /home/spark/jobs/<layer>/...   │
 │ airflow-dag-processor-dungafk            │    exec    │      │                                        │
 │ airflow-triggerer-dungafk                │  (socket)  │      ▼                                        │
 │ airflow-postgres-dungafk  metadata DB    │            │ spark-master / worker → Iceberg on MinIO      │
 └──────────────────────────────────────────┘            └───────────────────────────────────────────────┘
```

- **One task = one LakeHouse job.** A task runs
  `docker exec spark-client-dungafk spark-submit /home/spark/jobs/<layer>/<domain>/<layer>__<table>.py <date options>`
  through the host Docker socket (mounted into the scheduler only; with LocalExecutor the scheduler runs the tasks).
  The task fails when spark-submit fails; the whole Spark output is in the task log.
- The two stacks are separate compose projects. Airflow does not join the LakeHouse network (it does not need to:
  `docker exec` goes through the Docker daemon). The LakeHouse stack must be up for jobs to run.
- Job code lives only in LakeHouse_Banking; this repo holds only scheduling.
- Metadata DB: its own Postgres (`airflow-postgres-dungafk`, no host port), apart from the banking source DB.
- Pool `spark_jobs` (2 slots) limits Spark jobs running at the same time on the one Spark worker.

## Layout

```
Airflow_Banking/
├── docker-compose.yml          # Airflow stack (based on the official 3.3.2 compose, LocalExecutor)
├── docker/airflow/Dockerfile   # apache/airflow:3.3.2 + docker CLI
├── .env / .env.example         # .env = real secrets, git-ignored
├── logs/                       # Airflow logs, git-ignored
├── known_issues/README.md
└── dags/
    ├── .airflowignore          # common/ and *_task.py are modules, not DAG files
    ├── common/
    │   ├── settings.py         # time zone, start date, container / jobs path, pool, default_args
    │   └── spark_job.py        # spark_job_task(): the docker exec task + date params / date options
    ├── bronze/<domain>/
    │   ├── bronze__<table>_dag.py    # WHEN and in which ORDER: schedule, params, task flow
    │   └── bronze__<table>_task.py   # WHAT: which LakeHouse job the task calls
    └── silver/{base,pii}/
        ├── silver__<table>_dag.py
        └── silver__<table>_task.py
```

**Two files per table** (user decision 2026-11-26): `<layer>__<table>_dag.py` holds the schedule and the task flow;
`<layer>__<table>_task.py` holds the task(s) that call the LakeHouse job. Today each DAG has one task; later a DAG
can grow to e.g. *check source flag → ingest → save flag* without touching the task file of the job.
DAG id = `<layer>__<table>` (same name as the LakeHouse job file).

## DAGs

| DAG | LakeHouse job | Dates | Schedule (Vietnam time) |
|---|---|---|---|
| bronze__customer | bronze/crm/bronze__customer.py | cob_dt only (snapshot) | 01:00 daily |
| bronze__account | bronze/core_banking/bronze__account.py | cob_dt only (snapshot) | 01:00 daily |
| bronze__core_account | bronze/core_banking/bronze__core_account.py | cob_dt only (snapshot) | 01:00 daily |
| bronze__transaction | bronze/core_banking/bronze__transaction.py | cob_dt / range / full | 01:00 daily |
| bronze__merchant_transaction | bronze/payment/bronze__merchant_transaction.py | cob_dt / range / full | 01:00 daily |
| silver__dim_date | silver/base/silver__dim_date.py | none | manual only |
| silver__dim_party | silver/base/silver__dim_party.py | cob_dt / range / full | 02:00 daily |
| silver__dim_account | silver/base/silver__dim_account.py | cob_dt / range / full | 02:00 daily |
| silver__dim_party_identity | silver/pii/silver__dim_party_identity.py | cob_dt / range / full | 02:15 daily |
| silver__dim_contact | silver/pii/silver__dim_contact.py | cob_dt / range / full | 02:15 daily |
| silver__fact_account_balance | silver/base/silver__fact_account_balance.py | cob_dt / range / full | 02:30 daily |
| silver__fact_transaction | silver/base/silver__fact_transaction.py | cob_dt / range / full | 02:30 daily |
| silver__fact_merchant_transaction | silver/base/silver__fact_merchant_transaction.py | cob_dt / range / full | 02:30 daily |

All DAGs start **paused**. Order between DAGs is only by clock time for now (no cross-DAG dependency yet).

**Which date a run passes** (`dags/common/spark_job.py`): params `from_dt`+`to_dt` → `--from-dt/--to-dt`;
`full_load=true` → no date (= full load in LakeHouse); `cob_dt` → `--cob-dt`; nothing (scheduled run) →
`--cob-dt` = run date in Vietnam time − 1 day.
