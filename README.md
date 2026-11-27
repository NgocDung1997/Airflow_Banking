# Airflow_Banking

Airflow for jobs in the Banking domain: Airflow 3 (LocalExecutor) in Docker that schedules the Spark jobs of
[LakeHouse_Banking](../LakeHouse_Banking) (bronze and silver layers), with a flag table so silver waits for bronze.

Commands: [how_to_run.md](how_to_run.md) · Problems already met: [known_issues/README.md](known_issues/README.md)

## Architecture

```
 Airflow_Banking (network airflow-net-dungafk)            LakeHouse_Banking (network lakehouse-net-dungafk)
 ┌──────────────────────────────────────────┐            ┌───────────────────────────────────────────────┐
 │ airflow-apiserver-dungafk   UI :8090     │            │ spark-client-dungafk  (driver, client mode)   │
 │ airflow-scheduler-dungafk   runs tasks ──┼── docker ──┼─▶ spark-submit /home/spark/jobs/<layer>/...   │
 │ airflow-dag-processor-dungafk            │    exec    │      │                                        │
 │ airflow-triggerer-dungafk                │  (socket)  │      ▼                                        │
 │ airflow-postgres-dungafk                 │            │ spark-master / worker → Iceberg on MinIO      │
 │   ├─ airflow_db   (Airflow metadata)     │            └───────────────────────────────────────────────┘
 │   └─ etl_control  (job flags)            │
 └──────────────────────────────────────────┘
```

- **One task = one LakeHouse job**: `docker exec spark-client-dungafk spark-submit /home/spark/jobs/<...>.py <dates>`
  through the host Docker socket (scheduler only; LocalExecutor runs tasks there). The task fails when spark-submit
  fails; the whole Spark output is in the task log.
- Job code lives only in LakeHouse_Banking; this repo holds scheduling, flags and the DAG generator.
- Pool `spark_jobs` (2 slots) limits Spark jobs running at the same time on the one Spark worker.

## DAGs (all generated, all start paused)

| DAG | Tasks | Schedule (Vietnam time) |
|---|---|---|
| bronze__crm | ingest_customer → domain_done | 01:00 |
| bronze__t24 | ingest_account → ingest_core_account → ingest_transaction → domain_done | 01:00 |
| bronze__payment | ingest_merchant_transaction → domain_done | 01:00 |
| bronze__los | ingest_loan_product → ingest_loan_application → ingest_loan_contract → ingest_repayment → domain_done | 01:00 |
| bronze__card | ingest_card → ingest_card_txn → domain_done | 01:00 |
| bronze__ref | ingest_branch → ingest_product → ingest_gl_mapping → domain_done | 01:00 |
| silver__dim_date | load_dim_date | manual only |
| silver__dim_dpd_classification | load_dim_dpd_classification | manual only |
| silver__dim_party | check_source_flags (bronze.crm.customer exact) → load_dim_party | 02:00 |
| silver__dim_account | check_source_flags (bronze.t24.account exact) → load_dim_account | 02:00 |
| silver__dim_loan_product | check_source_flags (bronze.los.loan_product exact) → load_dim_loan_product | 02:00 |
| silver__dim_loan_contract | check_source_flags (bronze.los.loan_contract exact) → load_dim_loan_contract | 02:00 |
| silver__dim_party_identity | check_source_flags (bronze.crm.customer exact) → load_dim_party_identity | 02:15 |
| silver__dim_contact | check_source_flags (bronze.crm.customer exact) → load_dim_contact | 02:15 |
| silver__fact_account_balance | check_source_flags (bronze.t24.account exact; silver.base.dim_account, dim_party latest) → load_fact_account_balance | 02:30 |
| silver__fact_transaction | check_source_flags (bronze.t24.transaction exact; silver.base.dim_account, dim_party latest) → load_fact_transaction | 02:30 |
| silver__fact_merchant_transaction | check_source_flags (bronze.payment.merchant_transaction exact) → load_fact_merchant_transaction | 02:30 |
| silver__fact_loan_balance | check_source_flags (bronze.los.loan_contract exact; dim_loan_contract, dim_loan_product, dim_party, dim_account latest) → load_fact_loan_balance | 02:30 |
| silver__fact_repayment | check_source_flags (bronze.los.repayment exact; dim_loan_contract, dim_loan_product, dim_party latest) → load_fact_repayment | 02:30 |
| silver__fact_loan_application | check_source_flags (bronze.los.loan_application, loan_contract exact; dim_party, dim_loan_product latest) → load_fact_loan_application | 02:30 |

**Bronze — one DAG per domain.** Tables run one after another in the order of `bronze_tables.csv`. A failed table
does **not** stop the next ones (`trigger_rule="all_done"`); the last task `domain_done` (`none_failed`) then becomes
`upstream_failed`, so the run shows as failed and the failed table is easy to spot (clear that task to re-run it).

**Silver — one DAG per table, with bronze + silver parent flag checks.** Parent dependencies come directly from
`job_description.job_parents` in each LakeHouse YAML (single source of truth; `silver_tables.csv` has no
`source_tables` column). `check_source_flags` is a sensor (reschedule mode, every 1 min, up to 4 h) with two modes:
- **`exact`** (bronze parents and silver `overwrite_partitions` / `append` parents): requires `SUCCESS` on `cob_dt = D`.
- **`latest`** (silver `scd1` / `scd2` / `scd3` dimension parents, encoded as `<table_name>@latest`): requires any
  `SUCCESS` flag on `cob_dt <= D`, so backfilling facts is not blocked by dim dates.

## Flags (`etl_control` database)

| Table | Content |
|---|---|
| `job_flag` | current state, one row per (layer, schema_name, table_name, cob_dt): status RUNNING / SUCCESS / FAILED, dag / run / task / try, started_at, finished_at, message |
| `job_flag_history` | every state change, append-only |

Every Spark task writes its own table's flags: RUNNING before spark-submit, then SUCCESS (exit 0) or FAILED.
Silver tables write flags too (`silver.<ns>.<table>`), ready for gold later. Code: `dags/common/flags.py`.

**Which dates** (`dags/common/spark_job.py`, `resolve_dates`):

| Run with | Job gets | Flags written / checked for |
|---|---|---|
| nothing (scheduled) | `--cob-dt` = run date (VN time) − 1 | that date |
| `cob_dt` | `--cob-dt X` | X |
| `from_dt` + `to_dt` | `--from-dt X --to-dt Y` (snapshot tables: **skipped**) | every day X..Y |
| `full_load` = true | no date (snapshot tables: T-1) | none (nothing to check) |
| `skip_flag_check` = true (silver) | as above | flags not checked |

## Generator

```
dags/auto/
├── generate_dags.py            # python dags/auto/generate_dags.py  (writes / updates / deletes generated files)
├── config/
│   ├── bronze_domains.csv      # domain, schedule, active
│   └── silver_tables.csv       # namespace, table, date_mode, schedule, active (parents from LakeHouse YAML job_parents)
└── templates/                  # job_task.py.tpl, bronze_domain_dag.py.tpl, silver_table_dag.py.tpl
```

Bronze tables come from **LakeHouse_Banking/jobs/auto/bronze_tables.csv** (the same list the LakeHouse bronze jobs
are generated from); snapshot → `COB_DT_ONLY`, incremental → `DATE_RANGE`. The generator checks that every LakeHouse
job file exists. Generated files start with `# GENERATED by dags/auto/generate_dags.py` — never edit them by hand.

## Layout

```
Airflow_Banking/
├── docker-compose.yml          # Airflow stack (based on the official 3.3.2 compose, LocalExecutor)
├── docker/airflow/Dockerfile   # apache/airflow:3.3.2 + docker CLI
├── .env / .env.example         # .env = real secrets, git-ignored
├── logs/                       # Airflow logs, git-ignored
├── known_issues/README.md
└── dags/
    ├── .airflowignore          # common/, auto/ and *_task.py are not DAG files
    ├── common/                 # settings.py, spark_job.py (tasks), flags.py (flag store)
    ├── auto/                   # generator + config + templates
    ├── bronze/<domain>/        # bronze__<domain>_dag.py + bronze__<table>_task.py   (generated)
    └── silver/{base,pii}/      # silver__<table>_dag.py + silver__<table>_task.py    (generated)
```
