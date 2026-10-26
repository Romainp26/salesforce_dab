# Runbook

How to deploy, run, monitor and fix the Salesforce ingestion. [architecture.md](architecture.md) explains the mechanics and [configuration.md](configuration.md) every setting. Examples use Roojai in PROD (`roojai_th_prod`, job `sf-nightly-roojai_th_prod`); swap in `uat` or `kumka` as needed.

## 1. Every morning

The nightly job starts at 00:30 Bangkok time. Signals:

| Signal | When | Meaning |
| --- | --- | --- |
| Job failure email (`alert_email`) | When the job fails | An object failed, a critical DQ check failed, or a task crashed. See [section 4](#4-when-something-fails) |
| "data quality warnings" SQL alert (`alert_email`) | 07:00 | A warning check failed in the last 24 h |
| "new fields digest" SQL alert (`data_owner_email`) | 08:00 | New fields were auto-added or await approval. See [5.5](#55-approve-or-reject-a-new-field) |
| Ops dashboard "Salesforce ingestion - roojai_th_prod" | Any time | Finish time, duration and failures per run; the latest run per object |

Quick health query:

```sql
SELECT status, count(*) AS objects, sum(rows_extracted) AS rows, sum(versions_written) AS versions
FROM roojai_th_prod.ops_salesforce.run_log
WHERE run_id = (SELECT max_by(run_id, started_at) FROM roojai_th_prod.ops_salesforce.run_log
                WHERE last_stage <> 'backfill')
GROUP BY status;
```

## 2. Running a job

- **UI**: Workflows → the job → *Run now*, or *Run now with different parameters* to set `objects`, `full_reload` or `workers`.
- **CLI**, from a machine with the Databricks CLI and workspace access:

  ```bash
  databricks bundle run -t prod sf_nightly_roojai              # as configured
  databricks jobs run-now <job_id> --json '{"job_parameters": {"objects": "Account,Opportunity", "full_reload": "Account"}}'
  ```

  Use `run-now` with JSON when a value has commas. `databricks jobs list --name sf-nightly-roojai_th_prod` gives the job ID.

Parameters (nightly): `objects` (subset, empty = all; runs paused objects too), `full_reload` (objects to extract in full), `workers` (objects at a time). A job allows one run at a time (`max_concurrent_runs: 1`): don't start a run while another one of the same job is in progress, and keep backfills outside the nightly window (both write the same tables).

## 3. Reading a run

Open the run's **ingest** task output. Each line carries the worker thread (`[ingest_3]`).

```
Ingesting 124 objects, 4 at a time, longest first: Task, Opportunity, Account, ...
Account: extracting (changes since 2026-10-25 17:30:00 UTC)
Account: extracted 1532 records in 1 pages via bulk (Bulk jobs 750J...)
Account: done in 74s: 1210 versions written (3 soft deletes, 0 archivals), newest SystemModstamp 2026-10-26 17:29:58 UTC
Progress: 37/124 objects finished
...
Ingest finished in 41 min: 123 succeeded, 0 failed, 1 skipped; 812345 records extracted, 501234 versions written
Per object:
object      status     seconds  extracted  versions  deleted  archived
...
FAILED Contract: BulkQueryError: Bulk job 750Z Failed: INVALID_DATE_FORMAT: Invalid DateTime
Details: SELECT object, last_stage, error_message FROM `roojai_th_prod`.`ops_salesforce`.`run_log` WHERE run_id = '...' AND status = 'failed'
```

The **preflight** output lists new, removed and retyped fields per object. The same facts are in the tables:

```sql
-- one row per object of a run
SELECT * FROM roojai_th_prod.ops_salesforce.run_log WHERE run_id = '<job run id>' ORDER BY status, object;
-- every step of one object
SELECT * FROM roojai_th_prod.ops_salesforce.run_events WHERE run_id = '<job run id>' AND object = 'Account' ORDER BY event_at;
-- check results
SELECT * FROM roojai_th_prod.ops_salesforce.dq_results WHERE run_id = '<job run id>' AND NOT passed;
-- current watermarks (UTC)
SELECT * FROM roojai_th_prod.ops_salesforce.watermarks ORDER BY object;
```

`run_log.last_stage` says where an object stopped: `extract`, `load`, `deletes`, `ingest` (done) or `backfill`.

## 4. When something fails

First rule: **repair the run** (Jobs UI → the run → *Repair run*). The ingest task skips objects that already finished in that run, and failed ones re-extract from their last committed watermark. Repairing is always safe: loads are idempotent and no watermark moves past uncommitted data.

| Symptom | Likely cause | Action |
| --- | --- | --- |
| Every object fails at `extract` with an auth error | Certificate expired or replaced, consumer key changed, integration user locked, login from a non-NAT IP | Check `ops_salesforce.certificate` and the External Client App. Fix the secret ([5.10](#510-renew-the-jwt-certificate)), then repair |
| An object is `skipped` at `last_stage = 'preflight'` with "not in the <env> org" (`object_planned` warning) | The object doesn't exist in that org, e.g. a PROD-only object in a UAT sandbox | Expected for a sandbox that lacks it. Otherwise check the integration user's object access. Pausing the object in that environment acknowledges it and stops the warning ([5.4](#54-pause-or-unpause-an-object)) |
| An object `failed` at `last_stage = 'preflight'` (`object_planned` warning) | Its `describe` failed (Salesforce 5xx, permissions), or its plan lacks `Id` or the watermark column | Read `error_message`. Fix the cause, then start a new run, or repair with the preflight task selected: a default repair only re-runs the failed tasks, and preflight succeeded. For the plan, allow `Id` and the watermark in `ops_salesforce.field_allowlist` |
| A task shows *Timed out* | The ingest (or backfill, seed) ran past `long_task_timeout_seconds`, or another task past 1 hour | The following tasks still ran, so the landing files are gone. Repair: finished objects are skipped. If it happens on a normal night, raise the timeout or the node size |
| One object fails at `extract` with "Bulk job ... still <state> after 180 minutes; aborted" | A Bulk query job stuck on Salesforce's side | Repair the run. If it repeats, check Salesforce's Bulk API job monitor for that object |
| One object fails at `extract` with a Salesforce error (`INVALID_FIELD`, `INVALID_DATE_FORMAT`, `QUERY_TIMEOUT`, ...) | Field removed or renamed, bad data in the org, an object the Bulk API can't query | Read `error_message`. A field problem shows in preflight's warnings: reject or exclude the field ([5.5](#55-approve-or-reject-a-new-field)). Bad data: pause the object ([5.4](#54-pause-or-unpause-an-object)) and raise it with the Salesforce admins. A record that can't be returned at all: add a `soql_filter` ([5.7](#57-add-change-or-remove-an-object)) |
| Object fails at `load` with "values that don't fit their column type, count per column: {...}" | A field's data doesn't match its describe type, e.g. after a type change | Check `field_retyped` warnings. Values are withheld because they can hold PII; inspect them in Salesforce. Once fixed, repair. A retyped field keeps its old column type: to change it, reset the object ([5.9](#59-reset-an-object)) |
| Object fails at `deletes` with "full extract has no rows" | User or GroupMember came back empty | Never accepted as "everything deleted". Check the integration user's access, then repair |
| `dq` fails `one_current_row_per_id` | Two current versions for an `Id`: a bug or a manual write | Find them with `SELECT Id FROM <table> WHERE _is_current GROUP BY Id HAVING count(*) > 1`, fix with `_valid_to`/`_is_current`, and report it |
| `dq` fails `pii_is_ciphertext` | Plaintext in a PII column: a column listed after it was loaded, re-encryption skipped, or a manual write | Count per column is in `detail`. Restrict access first; check `ops_salesforce.encryption_state` for the object |
| Warning `count_matches_soql` | Records changed between the extract and the `COUNT()`, or duplicates across chunks | Usually harmless if small; investigate repeated or large gaps |
| Warning `watermark_advanced` | An object wrote versions but its watermark didn't move | Check `run_events` for that object |
| Warning `landing_volume_empty` | Files left in the landing volume (plaintext PII) | Delete them: `/Volumes/<catalog>/ops_salesforce/landing/<object>/<run_id>/` |
| Warning `field_removed`, `field_retyped` | Salesforce schema change | Removed: the column stays, null from now on. Retyped: see the load row above |
| Warning `certificate_expiry` | Less than 30 days left | [5.10](#510-renew-the-jwt-certificate) |
| Ingest output warns "the Bulk API doesn't support this object" | The object fell back to REST | Set `api: rest` on it |
| Job takes much longer than usual | A big object, Salesforce throttling (429 retries in the output), the single node saturated | Per-object table in the output. Raise `workers` if the node is idle while waiting on Salesforce; otherwise choose a bigger `node_type_id` in `sf_cluster` |

A night with no run (cluster outage, paused schedule) is caught up by the next run from the watermark. Gaps longer than 15 days lose deletions made early in the gap, because Salesforce empties its recycle bin.

## 5. Procedures

### 5.1 Deploy a change
1. Open a pull request. CI runs `uv run pytest` and `databricks bundle validate -t uat` and `-t prod`.
2. Merge to main: CI deploys to `uat` and runs the UAT job.
3. Release tag with approval: CI deploys to `prod`.

Manual equivalent: `databricks bundle deploy -t uat|prod`. A deploy uploads the wheel and the config; the next run uses them.

### 5.2 Full reload of an object
Run the nightly job with `objects` and `full_reload` set to the object, e.g. `Account`. It extracts every record and merges like any night: changed records get a version, nothing is overwritten, and the watermark never moves back. For very large objects, set `split_full_reload: true` first (yearly ranges in parallel).

### 5.3 Re-run one object
Run the nightly job with `objects` set to it. It extracts from its watermark as usual.

### 5.4 Pause or unpause an object
Add `paused: [uat]` (or `[prod]`, or both) to the object in `config/objects/<entity>.yml` and deploy. The nightly run logs it as `skipped`. Running the job with the object in `objects` still runs it, to test a fix. Remove the key to unpause. A paused object's watermark doesn't move, so it catches up when unpaused, unless more than 15 days pass, which loses deletions.

### 5.5 Approve or reject a new field
New fields on `require-approval` objects wait in `ops_salesforce.field_approval`:

```sql
SELECT * FROM roojai_th_prod.ops_salesforce.field_approval WHERE status = 'pending';

UPDATE roojai_th_prod.ops_salesforce.field_approval
SET status = 'approved', decided_by = '<you>', decided_at = current_timestamp()   -- or 'rejected'
WHERE object = 'Account' AND field = 'New_Field__c' AND status = 'pending';
```

The next run applies it: approved fields are queried from then on, rejected ones never. **If the field holds PII**, add it to the object's `encrypted_fields` in the same release, before the next run. To fill the field on existing records, run a backfill ([5.6](#56-backfill-a-field)).

Excluding a field that is already allowed:
- `UPDATE ... field_allowlist SET allowed = 0, source = 'manual', updated_at = current_timestamp() WHERE object = ... AND field = ...`
- its column stays, null for new versions.

### 5.6 Backfill a field
For a field approved or auto-added after its records were last extracted.

1. Run `sf-backfill-roojai_th_prod` with `fields` = `Object.Field,...`, outside the nightly window.
2. It extracts `Id` plus those fields from every record, plus the fields a conditional encryption rule needs (Account `Name` needs `Id` and `Type`).
3. It encrypts as configured and updates the **current versions only**.

Closed versions keep null, watermarks don't move, and records missing from the table are skipped. `run_log` shows `last_stage = 'backfill'` and `versions_written` = rows updated. `Id`, the watermark column and `*History` objects can't be backfilled.

### 5.7 Add, change or remove an object
- **Add**:
  1. Append it to `config/objects/<entity>.yml` with its policies.
  2. Add its fields to `config/seed/<entity>_fields_allowlist.csv`, including `Id` and its watermark column.
  3. Deploy. The next run loads it in full (no watermark yet).
- **Change a policy** (`delete_policy`, `include_created_today`, `view_filter`): edit and deploy. The views task recreates the view; tables are unaffected.
- **`soql_filter`**: only for records whose query fails in Salesforce. Records it excludes are never extracted. Use `view_filter` for anything else.
- **Remove**: delete it from the YAML. Its table, view and ops rows stay until dropped by hand.

### 5.8 Add a PII field
1. List it in `encrypted_fields`, or in a `conditional_encryption` or `history_encryption` rule. An object with `encrypted_fields` or `conditional_encryption` must also have `new_fields: require-approval`; config validation refuses it otherwise.
2. Deploy.

The next load encrypts its existing values in every version once, then every new value; `pii_is_ciphertext` verifies it. Changing a `when` rule doesn't re-encrypt existing rows: reset the object ([5.9](#59-reset-an-object)) if it must.

### 5.9 Reset an object
Rebuilds an object's table from Salesforce. History before the reset is lost, so take a copy first if needed.

```sql
CREATE TABLE roojai_th_prod.ops_salesforce.backup_account DEEP CLONE roojai_th_prod.bronze_salesforce_scd2.account;  -- optional
DROP TABLE roojai_th_prod.bronze_salesforce_scd2.account;
DELETE FROM roojai_th_prod.ops_salesforce.watermark_history WHERE object = 'Account';
DELETE FROM roojai_th_prod.ops_salesforce.encryption_state WHERE object = 'Account';
DELETE FROM roojai_th_prod.ops_salesforce.view_state WHERE object = 'Account';
```

Then run the nightly job with `objects = Account`. It loads every record as a first version with today's types and PII rules.

### 5.10 Renew the JWT certificate
1. Create the new key and certificate.
2. Upload the certificate to the org's External Client App, keeping the old one until the switch.
3. Replace the private key secret (`entities.<entity>.secrets.private_key`): PEM key followed by its certificate, or base64 PFX.
4. Run the job with `objects` set to one small object (e.g. `UserRole`) to check.

Preflight records the new expiry in `ops_salesforce.certificate`.

### 5.11 After a UAT sandbox refresh
- **External Client App:** a refresh removes locally created External Client Apps. Redeploy the app (as packaged metadata) or recreate it with the same certificate, and check the consumer key secret.
- **Integration user:** check its profile, IP restriction and permissions.
- **Data:** the sandbox now holds a new copy of PROD. The next nightly run catches up from the old watermarks, so changes made before them are missed. To match the new copy, reset the UAT objects ([5.9](#59-reset-an-object)), or drop and recreate the UAT catalog's schemas and run.

### 5.12 Rotate the PII key
Not automated. Rotation changes every ciphertext (design: Keys):
1. Plan a maintenance window with the job paused.
2. Store the new key and IV under new secret names.
3. Re-encrypt every PII column of every table, under the same rules as `dq.pii_predicates`, for example:

   ```sql
   UPDATE <table> SET <column> = base64(aes_encrypt(
       CAST(aes_decrypt(unbase64(<column>), unbase64(secret('<scope>', '<old key>')), 'CBC', 'PKCS') AS STRING),
       unbase64(secret('<scope>', '<new key>')), 'CBC', 'PKCS', unbase64(secret('<scope>', '<new iv>'))))
   WHERE <column> IS NOT NULL;
   ```

4. Point `pii.aes_key_secret` / `aes_iv_secret` at the new secrets, bump `pii.key_version` and update `_pii_key_version`.
5. Deploy and run preflight, which recreates `decrypt_pii`; the `pii_is_ciphertext` check and a sample decrypt confirm it.

Write and test the script in UAT first ([open-questions.md](open-questions.md)).

### 5.13 Initial load (once per environment)
1. **Keys.** The platform team stores the new PII key and IV (`pii.aes_key_secret`, `aes_iv_secret`; commands to generate them are in `entities.yml`), and grants the service principal READ on the scope holding the legacy key (`pii.legacy`).
2. **Clone.** Deep-clone each Synapse table into `<catalog>.backup_salesforce`, named like its SCD2 table:

   ```sql
   CREATE SCHEMA IF NOT EXISTS roojai_th_prod.backup_salesforce;
   CREATE TABLE roojai_th_prod.backup_salesforce.account DEEP CLONE <synapse copy of Account>;
   ```

   Synapse tables and columns already carry the Salesforce API names, and its timestamps hold Bangkok time, so a plain deep clone is all the seed needs. Only the service principal and the engineers doing the load should read this schema.
3. **Seed.** Deploy and run `sf-seed-roojai_th_<env>`, optionally with `objects` to seed a subset. Per object, its output says how many rows were seeded, the seeded watermark and which fields the backup lacked.
   - It refuses non-empty tables.
   - An object without a backup table is skipped; its first nightly run loads it in full.
   - An object fails, with counts per column, when PII doesn't decrypt with the legacy key or values don't fit their type. Its table stays empty, so after the fix, re-run with `objects` set to it.
4. **Check** `dq_results` where `check_name = 'seed_count'` (backup rows = loaded rows).
5. **Go.** Unpause the nightly schedule. The first run must follow **within 15 days** of the Synapse cut-off, while Salesforce's recycle bin still holds the deletions made in between.
6. Once every environment is seeded, remove `pii.legacy` from `entities.yml`. Keep or drop `backup_salesforce` as the retention policy says.

### 5.14 Onboard Kumka (phase 2)
1. **Config:** create `config/objects/kumka.yml` and `config/seed/kumka_fields_allowlist.csv` (e.g. with `scripts/generate_config.py --entity kumka`), and fill `entities.kumka` in `entities.yml`: orgs, usernames, secrets, `archival_user_ids`.
2. **Jobs:** copy every `resources/*roojai*` definition (nightly, backfill, seed, alerts, dashboard) with `roojai` replaced by `kumka`.
3. **Platform:** the platform team creates `kumka_th_uat` / `kumka_th_prod`, grants the service principals and creates `pii_readers_kumka_th_<env>`.
4. Follow [uat-checklist.md](uat-checklist.md), then 5.13.

### 5.15 Table maintenance
Predictive optimization, enabled by preflight on `bronze_salesforce_scd2` and `ops_salesforce`, runs `OPTIMIZE`, `VACUUM` and statistics on the managed tables. There is no maintenance job.
- **Is it on?** `DESCRIBE SCHEMA EXTENDED roojai_th_prod.bronze_salesforce_scd2` should show it enabled. A failure to enable it appears as a `setup` warning in `dq_results`.
- **What did it do?** `system.storage.predictive_optimization_operations_history`.
- **Time travel.** `VACUUM` keeps 7 days of history by default, so `RESTORE` and time travel reach back a week.

### 5.16 Upgrade the Databricks runtime
1. Change `sf_cluster.spark_version` in `databricks.yml`.
2. Align the local test versions in `pyproject.toml`: `pyspark` and `delta-spark` matching the runtime's Spark.
3. Run the tests.
4. Deploy to UAT and compare a night's `run_log` and a sample of ciphertext with the previous runtime before PROD.

## 6. Access

| Who | Grants |
| --- | --- |
| Environment service principal | Owns everything in its catalog; the only reader of the landing volume |
| BI and data science | SELECT on `bronze_salesforce` (views); `bronze_salesforce_scd2` for history and the SCD2 flags (`_is_deleted`, `_is_archived`, `_valid_from`, ...) |
| Initial load engineers | `backup_salesforce`, during the load only: it holds legacy ciphertext and plaintext non-PII |
| `pii_readers_<catalog>` | EXECUTE on `ops_salesforce.decrypt_pii` and READ on the secret scope: `SELECT ops_salesforce.decrypt_pii(Active_Email__c) FROM ...` |
| Operators | SELECT on `ops_salesforce`; MODIFY on `field_approval` (and `field_allowlist` for exclusions); CAN MANAGE RUN on the jobs |

No user gets write access to PROD tables or read access to the landing volume.

## 7. Local development

```bash
uv sync                          # Python 3.12; local Spark 4.1 + Delta for tests
uv run pytest                    # Spark tests need Java 17+; -m "not spark" skips them
uv run ruff check . && uv run ruff format .
uv build --wheel
```

Tests run Spark and Delta locally against the `spark_catalog`, with Salesforce and secrets replaced by fakes (`tests/test_tasks.py`). Unity Catalog statements (volume, function, grants) only run on Databricks.
