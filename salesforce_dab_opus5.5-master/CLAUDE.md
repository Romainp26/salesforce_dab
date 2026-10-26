# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Databricks Asset Bundle that ingests Salesforce (Roojai now, Kumka in phase 2) into Unity Catalog nightly. The agreed design is `shared_folder/salesforce-ingestion-design.md`. Code comments cite its IDs (FR-xx, NFR-xx, D-xx), so check the design before changing behaviour. `shared_folder/` is where the user drops input files.

## Commands

- `uv sync`: install (Python 3.12, matching Databricks Runtime 18 LTS: Spark 4.1.0, Scala 2.13). Dependencies are capped below their next major version; raise a cap only after testing the new major.
- `uv run pytest`: all tests. Spark tests run local Spark 4.1 + Delta and need Java 17+. The user's shell `JAVA_HOME` may point at a removed JDK, so prefix with `JAVA_HOME=$(/usr/libexec/java_home -v 17)`.
- `uv run pytest tests/test_scd2.py::test_changes_overlap_and_replays`: a single test. `-m "not spark"` skips the Spark ones.
- `uv run ruff check . && uv run ruff format .`: lint and format (line length 120).
- `uv build --wheel`: the wheel the jobs install. Its entry point `sf_ingest` takes the task name as its first argument.
- `databricks bundle validate|deploy -t uat|prod`: needs the Databricks CLI and workspace auth. The CLI is not installed locally.

## Documentation

`docs/` is the durable record, written for someone new to the project: `architecture.md`, `configuration.md`, `runbook.md`, `uat-checklist.md`, `requirements.md` (traceability to the design), `decision-log.md`, `open-questions.md`. A change to behaviour, configuration or operations updates the matching document in the same change. A decision that departs from the design gets a B-xx entry with its alternatives, and an answered question leaves `open-questions.md`.

## Architecture

**Job flow** (`resources/sf_nightly.job.yml`, one job per catalog, every task on one classic job cluster):
`preflight → ingest → views → dq → cleanup`. `resources/sf_backfill.job.yml` runs `preflight → backfill → views → cleanup`; `resources/sf_seed.job.yml` (one-off initial load from `backup_salesforce`) runs `preflight → seed → views`. No maintenance job: preflight enables predictive optimization.
- Each task is a subcommand in `src/sf_ingest/tasks.py`. Tasks after ingest run with `run_if: ALL_DONE`. Every task has a `timeout_seconds`, per task and never per job: a job timeout would cancel `cleanup` and leave plaintext landing files (B-20).
- Preflight writes the per-object field list to `ops_salesforce.run_plan`, keyed by `run_id` (the job run id).
- `tasks.ingest` runs every object in a thread pool (`--workers`), ordered by `longest_first`. Per object, `ingest_object` runs extract → load → hard deletes (full loads) → deletes the landing folder, then appends an `ingest` event. A failed object is logged and the task raises at the end. A repair of the same run skips objects that already have an `ingest` event. Each worker thread gets its own FAIR scheduler pool and its own Salesforce session (`Context.session`).
- The ingest output is the operator's view of the night: keep the per-object lines and the summary table in `log_summary` when changing it.
- This departs from the design's for-each tasks; `docs/decision-log.md` (B-xx) records every departure from the design.

**Data path:**
- `extract.py`/`bulk.py` stream each Bulk API result page unparsed to `/Volumes/<catalog>/ops_salesforce/landing/<object>/<run_id>/page_<n>.csv`.
- `scd2.py:load_object` reads the pages as strings, then:
  - dedupes per Id
  - hashes the plaintext tracked fields
  - sets delete/archive flags, encrypts PII, casts types (datetimes become `TIMESTAMP_NTZ` +7h Bangkok)
  - runs a staged-union MERGE.
- `*History` objects (name ends in `History`) are append-only instead.
- Watermarks stay UTC and advance only after the MERGE commits.

**Invariants that are easy to break:**
- **Idempotency.** An incoming row only becomes a version if its `_valid_from` is newer than the current one and its `_row_hash` differs. Replays and the 1-hour overlap are therefore no-ops.
- **One object never fails preflight.** `tasks.preflight` gives an object it can't describe or plan no `run_plan` row and a `preflight` event: `skipped` when the org lacks it, `failed` otherwise. Ingest reports those objects and fails at the end for the failed ones (B-19). Only setup and authentication errors fail preflight.
- **Partial extracts are never loaded.** `ingest_object` only loads after `extract` returns; an extract error skips the load and the watermark stays.
- **Concurrent writes.** Worker threads only append to `ops_salesforce.run_events` and `watermark_history`. The `run_log` and `watermarks` views aggregate them. Don't turn these into MERGEs. `run_log.status` is the latest event's status, so a repaired object reads `succeeded`.
- **Thread safety.** Session-wide Spark settings (`spark.conf.set`) are shared by every worker thread: avoid them in per-object code. The re-encryption commit tag is the one exception, so it is serialised and names its table. Temp views get unique names.
- **Casts never throw.** `transform.cast_from_string` uses `try_cast`. `scd2.profile_landed` fails the object when any value doesn't fit, with counts per column only: an ANSI cast error would put the value, possibly PII, in the run log.
- **One place for crypto.** All encryption SQL comes from `encrypt.py:CipherSpec`, which reads the key and IV via `secret()` in prod. Format (B-13): random base64 key and IV, Spark's IV-prefixed output, no base64 line breaks. `CipherSpec.legacy` / `decrypt_any_sql` read the old key's ciphertext and are for the seed only.
- **PII objects require approval.** Config validation refuses `encrypted_fields` / `conditional_encryption` without `new_fields: require-approval`. `IsDeleted` and `LastModifiedById` are always queried (`metadata.SYSTEM_FIELDS`).
- **Views expose business columns only.** SCD2 columns (`_is_deleted`, `_valid_from`, ...) stay in `bronze_salesforce_scd2`.
- **Re-encryption runs at most once per column.** Newly listed PII columns are re-encrypted once, tracked in `ops_salesforce.encryption_state` and tagged in the Delta commit metadata. Never encrypt a column outside that path; the field backfill calls it before merging.
- **Hard deletes.** Only full-load objects (User, GroupMember) are checked, after their load. `scd2.changed_rows` makes a flagged record that is back in the extract current again.
- **No named SQL parameters.** `spark.sql(..., args=...)` comes back unbound with Spark 4.1 + Delta 4.3 on Delta tables and views. Filter with the DataFrame API, or build literals with `ops.sql_string`.
- **Plaintext stays out of logs.** Landing files hold plaintext PII. Log counts and IDs only, never field values, and never `display()`/`show()` in job code.

**Config:**
- `config/objects/<entity>.yml` is the reviewed source of truth for object policies (`ObjectConfig` in `config.py` lists the keys, including `paused: [uat|prod]`). `soql_filter` is only for records Salesforce can't return (the Contract corrupted record); business exclusions go in `view_filter` (D-17).
- It was bootstrapped once by `scripts/generate_config.py` from the CSVs in `shared_folder/`, which refuses to overwrite without `--force`.
- The field allowlist is seeded from `config/seed/` but changes at run time (auto-adds, approvals) in `ops_salesforce.field_allowlist`.
- Catalog names are derived as `<entity>_<country>_<env>`.

**Tests** use the local `spark_catalog` as the catalog. `tests/test_tasks.py:FakeContext` stands in for secrets and Salesforce, and `FakeSalesforce` replaces `extract_object` with pages given per object. Unity Catalog-only statements (volume, function, grants) live in `ops.ensure_uc_objects` and aren't exercised locally. `docs/uat-checklist.md` names the tests behind each UAT case.
