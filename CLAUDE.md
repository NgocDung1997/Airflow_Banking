# CLAUDE.md — Airflow_Banking

Read this first in every new session. Architecture and layout: [README.md](README.md); commands: [how_to_run.md](how_to_run.md);
problems already met: [known_issues/README.md](known_issues/README.md). The jobs themselves live in
`E:\Study\LakeHouseProject\LakeHouse_Banking` — read its CLAUDE.md before changing how a job is called.

**Known issues rule:** when an Airflow / DAG error is solved, add a `KI-xxx` entry (You see / Why / Fix) + an Index
line in `known_issues/README.md` in the same turn. Never add git / Git Bash issues, port clashes, or locked files.

---

## 1. Working rules

### Communication
- Reply to the user in **Vietnamese**. Everything written into the project is **English** (code, comments, logs, `.md`, commits).

### Ideas first, build after approval
- When the user asks for an idea / proposal / plan, **only propose**; change nothing until they approve ("chốt", "duyệt", "làm đi").

### Drive C is off-limits
- Do not create, edit or store anything on drive C (no scratchpad, memory or temp files).
  Temporary files go to `E:\Study\LakeHouseProject\_tmp\`; delete them when done. Persistent notes live in this file.

### Credentials Excel — mandatory
- File: `E:\Study\LakeHouseProject\LakeHouse_Banking_Credentials.xlsx` (shared with LakeHouse_Banking, outside both repos).
- Whenever an Airflow service / container / port (`docker-compose.yml`) or a login / password / database (`.env`) is
  added, changed or removed: update sheets **Accounts** / **Services**, append a **Change Log** row, tell the user.
  Same rules as LakeHouse_Banking/CLAUDE.md (openpyxl, keep the style, never print passwords, "(see Accounts)" in Change Log).

### DAG layout (user decision 2026-11-26)
- Everything Airflow runs lives in `dags/`. **Two files per table** in `dags/<layer>/<domain>/`:
  `<layer>__<table>_dag.py` = schedule, params, task flow; `<layer>__<table>_task.py` = the task(s) calling the
  LakeHouse job. DAG id = `<layer>__<table>`. Shared code in `dags/common/`.
- Tasks call jobs only with **`docker exec spark-client-dungafk spark-submit ...`** (user decision) — no Spark inside
  Airflow (the Spark images run Python 3.8, Airflow 3 needs ≥ 3.9; driver and executors must match).
- Planned (not built yet, ask before building): per-table flow *check source flag → ingest → save flag*.
- `common/` and `*_task.py` are listed in `dags/.airflowignore` (modules, not DAG files).

### Secrets & git
- `.env` is git-ignored — never commit it, never hard-code its values. Keep `.env.example` keys in sync.
- Do not commit or push unless the user asks; check `git status` that `.env` is not staged. Line endings LF.

### Changes to the stack
- After changing `.env` / `docker-compose.yml`: `docker compose up -d --force-recreate`. Destructive actions
  (`down -v`, dropping the metadata DB) need the user's OK. Verify after every change and report honestly.
- Testing a DAG writes real LakeHouse tables (there is no test mode): re-run a date already loaded
  (`airflow dags test <dag> -c '{"cob_dt": "..."}'`) and compare counts in Trino.

---

## 2. Project facts / gotchas

- Airflow **3.3.2**, LocalExecutor, FAB auth manager, image `airflow-banking:3.3.2` (official image + docker CLI 27).
- Containers `airflow-*-dungafk`, network `airflow-net-dungafk` (does NOT join `lakehouse-net-dungafk`).
  UI on host port **8090** (8080 = Spark Master). Metadata Postgres 16 has no host port.
- Docker socket is mounted into the **scheduler only** (LocalExecutor runs tasks there). Access works because the
  socket is `root:root 660` and Airflow runs as `50000:0` (group root).
- Default time zone `Asia/Ho_Chi_Minh`. Scheduled runs pass `--cob-dt` = run date (VN time) − 1 day, computed from
  `dag_run.run_after` (not `ds`, which is UTC and can be null on manual runs in Airflow 3).
- Bronze snapshot jobs accept `--cob-dt` only (`COB_DT_ONLY`); `silver__dim_date` takes no date (`NO_DATE`, manual DAG).
- Pool `spark_jobs` (2 slots) is created by `airflow-init-dungafk` from `SPARK_POOL` / `SPARK_POOL_SLOTS`.
- From Git Bash, `docker exec -w /opt/...` needs `MSYS_NO_PATHCONV=1`.
- Metadata DB user / password and UI login are changed on the running system (no `down -v`): UI →
  `airflow users reset-password`; DB password → `ALTER ROLE`; DB user name → stop airflow-* containers, temporary
  superuser, `ALTER ROLE <old> RENAME TO <new>` + set the password again (rename clears it), drop the temp role.
  Then update `.env` and `docker compose up -d --force-recreate`. Done 2026-11-26: UI `admin`, DB `airflow_dungnn`.

---

## 3. Current status

**Last updated:** 2026-11-26
- Stack up and healthy; 13 DAGs (5 bronze, 8 silver) parsed with no import errors, all paused.
- Verified: `bronze__customer` with cob_dt 2026-11-23 → task ran spark-submit, exit 0; Trino `bronze.crm.customer` still 20 rows.
- Not built yet: cross-DAG dependencies (bronze → silver, dims → facts; today only staggered times), flag checks, DQ, gold.
