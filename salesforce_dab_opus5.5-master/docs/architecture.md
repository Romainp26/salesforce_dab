# Architecture

How the solution works, end to end. The agreed requirements and design are in `shared_folder/salesforce-ingestion-design.md` (IDs FR-xx, NFR-xx, D-xx). This bundle implements that design with the departures listed in [decision-log.md](decision-log.md) (IDs B-xx).

## Overview

```mermaid
flowchart LR
  SF["Salesforce org<br/>Bulk API 2.0 queryAll<br/>REST for metadata"]
  subgraph JC["Classic job cluster, behind the NAT gateway's fixed IP"]
    PF[preflight] --> IN["ingest<br/>N objects in parallel"] --> VW[views] --> DQ[dq] --> CL[cleanup]
  end
  subgraph UC["Unity Catalog: one catalog per entity and environment, e.g. roojai_th_prod"]
    LV["ops_salesforce.landing (volume)<br/>raw CSV pages, plaintext, deleted after each object"]
    SCD["bronze_salesforce_scd2<br/>one SCD2 table per object, PII encrypted"]
    V["bronze_salesforce<br/>one policy view per object"]
    OPS["ops_salesforce<br/>run log, watermarks, allowlist, approvals, DQ results"]
  end
  KV[("Key Vault secret scope<br/>JWT key, AES key + IV")]
  SF -- pages --> LV -- "encrypt, MERGE" --> SCD --> V --> BI["BI and data science"]
  KV -.-> JC
  JC -.-> OPS
```

- One **job per catalog**: `roojai_th_uat`, `roojai_th_prod`, and later `kumka_th_*`. A catalog holds exactly one Salesforce org (D-10, D-16).
- Every task runs on **one classic job cluster**: Databricks Runtime 18 LTS with Photon, a single node (`Standard_D4ds_v5`), single-user mode as the environment's service principal, local disk encryption. Salesforce only accepts the NAT gateway's IP, and serverless compute can't use it (D-03, D-04).
- Code is one Python package, `sf_ingest`, built as a wheel. Each job task is a subcommand of its `sf_ingest` entry point (`src/sf_ingest/tasks.py`).

## Jobs

| Job (`resources/`) | Schedule | Tasks | Purpose |
| --- | --- | --- | --- |
| `sf_nightly_roojai` | 00:30 Asia/Bangkok | preflight → ingest → views → dq → cleanup | The nightly load |
| `sf_backfill_roojai` | on demand | preflight → backfill → views → cleanup | Fill newly approved fields on existing records |
| `sf_seed_roojai` | on demand, once | preflight → seed → views | Initial load from `backup_salesforce`, the clone of the Synapse tables |

Table maintenance (`OPTIMIZE`, `VACUUM`, statistics) is left to **predictive optimization**, which preflight enables on the SCD2 and ops schemas; there is no maintenance job.

All jobs have `max_concurrent_runs: 1` and email `alert_email` on failure. Tasks after `ingest` (and `backfill`) run with `run_if: ALL_DONE`, so views, checks and cleanup happen even when objects fail.

Every task has a `timeout_seconds` (B-20): `long_task_timeout_seconds` (6 hours) for ingest, backfill and seed, 1 hour for the others. The timeouts are per task, not per job, so a timed-out ingest still lets views, dq and cleanup run, and the landing files are deleted. Within ingest, the Bulk client aborts a query job still running after 3 hours (`bulk.MAX_JOB_WAIT_SECONDS`), which fails only that object.

### preflight
1. Creates the schemas, ops tables, landing volume and the `decrypt_pii` function if missing; grants EXECUTE on the function to `pii_readers_<catalog>`; enables predictive optimization on `bronze_salesforce_scd2` and `ops_salesforce`; deletes `run_plan` rows older than 30 days.
2. Seeds `ops_salesforce.field_allowlist` from `config/seed/<entity>_fields_allowlist.csv`. It only inserts missing rows, never overwriting run-time changes.
3. Applies decided approvals from `ops_salesforce.field_approval` to the allowlist.
4. Authenticates (JWT) and records the certificate's expiry; warns 30 days ahead.
5. Calls `describe` on every configured object (8 in parallel), then decides tonight's field list per object (`metadata.plan_fields`):
   - an allowed field (`allowed = 1`) is queried; an excluded field (`allowed = 0`) is never queried or reported
   - `IsDeleted` and `LastModifiedById` are always queried when the object has them, whatever the allowlist says: delete and archival detection read them
   - an unknown field on an `auto-add` object is added to the allowlist and queried tonight. No such object holds PII: config validation requires `require-approval` on every object with `encrypted_fields` or `conditional_encryption`
   - an unknown field on a `require-approval` object is recorded as `pending` and left out
   - compound (`address`, `location`) and `base64` fields are skipped
   - removed or retyped fields raise warnings. A removed field's column stays, null from then on; a retyped one keeps its old column type.
6. Writes the plan to `ops_salesforce.run_plan` (keyed by run ID) and the describe output to `ops_salesforce.object_schema`.

One object never fails preflight for the others (B-19). An object whose `describe` returns 404 (not in this org) gets no plan and a `preflight` / `skipped` event. Any other describe error, or a plan that lacks `Id` or the watermark column, gives a `preflight` / `failed` event with the error. Both raise an `object_planned` warning, except a missing object paused in that environment, and the object keeps its previous `object_schema` snapshot. Ingest reports these objects without running them; a failed one fails the ingest task at the end, so the job alerts. Only setup and authentication errors fail preflight itself.

### ingest
Runs every configured object (or the `objects` parameter's subset) in a thread pool, `workers` at a time (default 4, the node's cores). Objects are ordered by their average duration over the last 14 days, longest first; objects with no history go first. Each worker thread has its own Salesforce session and its own Spark FAIR scheduler pool.

Skipped:
- objects paused in this environment (`paused`), unless named in `objects`
- objects that already finished in this job run (a repair).

Per object (`tasks.ingest_object`):

1. **Extract** (`extract.py`, `bulk.py`). One Bulk API 2.0 `queryAll` job selecting tonight's fields:
   - `queryAll` also returns records in the recycle bin, with `IsDeleted = true`.
   - WHERE clause: the object's `soql_filter` if any, then, for incremental objects, `watermark > stored watermark − 1 hour` and `watermark <= now`. Full loads and full reloads have no watermark condition. A full reload of an object with `split_full_reload` runs yearly `CreatedDate` ranges, 4 at a time.
   - Each result page (`bulk_page_size` records) streams to `/Volumes/<catalog>/ops_salesforce/landing/<object>/<run_id>/page_<n>.csv` exactly as Salesforce returns it, so memory stays flat.
   - Retries: a page whose download drops is fetched again by its locator; 429 and 5xx responses back off and retry up to 5 attempts; a 401 re-authenticates once.
   - Objects configured `api: rest`, and objects the Bulk API rejects as unsupported, use REST `queryAll` and land the same CSV format.
   - A `SELECT COUNT()` with the same WHERE clause is recorded for the reconciliation check.
2. **Load** (`scd2.load_object`), detailed in [SCD2 tables](#scd2-tables). Reads the pages as strings and checks that every value fits its column type, else the object fails before anything is written. Then it dedupes per `Id`, flags deletes and archivals, hashes, encrypts PII, casts and MERGEs. After the MERGE commits, the watermark advances to the highest landed watermark (UTC) if that is later than the stored one.
3. **Hard deletes**, only for `load_type: full` objects (User, GroupMember). Current records absent from tonight's complete extract get a new version flagged `_is_deleted`. An empty extract fails the object instead of flagging everything.
4. The object's landing folder is deleted, whatever happened.

Every step appends to `ops_salesforce.run_events`; a final `ingest` event marks the object done. A failed object is logged and the others continue; the task fails at the end if any object failed, so the job's failure email fires.

### views
Recreates a view only when its SQL changed (hash kept in `ops_salesforce.view_state`), for example after a new column or a config change.

### dq
Writes results to `ops_salesforce.dq_results`. A failed **critical** check fails the task, and with it the job:

| Check | Severity | Rule |
| --- | --- | --- |
| `one_current_row_per_id` | critical | No `Id` has two current versions |
| `pii_is_ciphertext` | critical | Every non-null value of every PII column (under its rule) decodes from base64 to whole AES blocks |
| `count_matches_soql` | warning | Rows extracted = `COUNT()` for the same window |
| `watermark_advanced` | warning | An incremental object that wrote versions advanced its watermark |
| `new_field_pending_approval` | warning | A field awaits approval (daily digest) |
| Written by other tasks | warning | `new_field_auto_added`, `field_removed`, `field_retyped`, `certificate_expiry`, `object_planned`, `setup` (preflight); `landing_volume_empty` (cleanup); `seed_count` (seed) |

Two Databricks SQL alerts read `dq_results` (`resources/sf_ops.alerts.yml`): warnings of the last 24 hours at 07:00 Bangkok to `alert_email`, and the new-fields digest at 08:00 to `data_owner_email`.

### cleanup
Deletes this run's landing folders, plus any folder older than 12 hours (a run whose cleanup never ran). Younger folders of other runs (a backfill running alongside) stay. Leftover files of this run or stale ones fail the `landing_volume_empty` check.

## Data model

Per catalog:

| Schema | Contents |
| --- | --- |
| `bronze_salesforce_scd2` | One table per object, named after the API name in lower case (`account`, `claim__c`). Every version of every record |
| `bronze_salesforce` | One view per object, same name: current versions with the object's policies applied |
| `ops_salesforce` | Control tables, the landing volume and `decrypt_pii` |

### SCD2 tables
Columns: every queried field, then:

| Column | Meaning |
| --- | --- |
| `_valid_from` | Start of the version: the record's watermark value (`SystemModstamp`) in Bangkok time |
| `_valid_to` | Start of the next version; null while current |
| `_is_current` | The latest version |
| `_is_deleted`, `_deleted_at` | Soft-deleted in Salesforce, or (User, GroupMember) missing from the full extract |
| `_is_archived`, `_archived_at` | Deleted by an archival user (`archival_user_ids`): archived, not deleted (D-23) |
| `_row_hash` | SHA-256 of the tracked fields' plaintext |
| `_run_id`, `_extracted_at`, `_page_no` | Job run that wrote the version, page time (Bangkok) and page number |
| `_pii_key_version` | Key version the row's PII was encrypted with |

Rules:
- **When a new version is written.** An incoming record becomes a new version only if it is a new `Id`, or its watermark is newer than the current version's and its hash differs. The hash excludes `hash_exclude` fields (default `SystemModstamp`, `LastViewedDate`, `LastReferencedDate`). So replays, the 1-hour overlap and full reloads write nothing for unchanged records (NFR-05). Within one extract the latest row per `Id` wins.
- **Hard deletes undo themselves.** A full-load record flagged as hard-deleted that is back in a later extract becomes current again, starting that night.
- **Deletes and archivals.**
  - A record returned with `IsDeleted = true` is archived when its `LastModifiedById` is an archival user, and soft-deleted otherwise.
  - Objects without `LastModifiedById` (feeds, `*History`, ProcessInstanceWorkitem) always count as soft-deleted.
  - An undelete arrives as a new version with both flags false.
- **Types.** Columns take the type `describe` reports:

  | Salesforce type | Column type |
  | --- | --- |
  | boolean | BOOLEAN |
  | int / long | INT / BIGINT |
  | double, currency, percent | `DECIMAL(precision, scale)`, else DOUBLE |
  | date | DATE |
  | datetime | TIMESTAMP_NTZ |
  | everything else, and every PII column | STRING |

  A value that doesn't fit its type fails the object, with counts per column and no values.
- **New fields** are added with `ALTER TABLE ADD COLUMNS`. Older versions hold null.
- **Table settings.** Tables are liquid-clustered on `Id` with deletion vectors.

`*History` objects (name ends in `History`) are **append-only** (FR-08). Their tables:
- hold the business fields plus `_run_id`, `_extracted_at`, `_page_no` and `_pii_key_version`
- only receive new `Id`s
- keep history Salesforce itself purges after its retention period.

### Timestamps
- Every Salesforce `datetime` is stored in **Bangkok time** (UTC + 7, no daylight saving) as `TIMESTAMP_NTZ`, which reads the same in any session time zone (FR-13, D-24). Date-only fields are not shifted.
- In `*History` tables, `OldValue` and `NewValue` are text. When `DataType = 'DateTime'` they are shifted too, written as `yyyy-MM-ddTHH:mm:ss.SSS` without the trailing `Z`.
- **Watermarks stay in UTC**, taken from the raw landed value, because SOQL compares in UTC.

### Policy views
`bronze_salesforce.<object>` returns the current versions with the business columns only; the SCD2 columns (`_is_deleted`, `_valid_from`, ...) stay in `bronze_salesforce_scd2`. Filters:

| Setting | Effect |
| --- | --- |
| `delete_policy: retain` (default) | Soft-deleted and archived records shown, with Salesforce's `IsDeleted = true`; `LastModifiedById` tells archivals apart |
| `delete_policy: delete` | Soft-deleted records hidden; archived ones still shown |
| `load_type: full` | Hard-deleted (missing) records hidden |
| `include_created_today: false` (default) | Records whose `CreatedDate` is today in Bangkok hidden, so the view stops at the end of yesterday |
| `view_filter` | Extra SQL condition |

On `*History` views, `delete_policy: delete` hides rows with `IsDeleted = true`. Consumers who need history query `bronze_salesforce_scd2` directly.

## PII encryption

- **Format.** `base64(aes_encrypt(value, key, 'CBC', 'PKCS', iv))` with a static IV, so identical values give identical ciphertext and joins and counts work on encrypted columns (D-07). Spark's native format, under a new key (B-13):
  - the key is 32 random bytes (AES-256) and the IV 16 random bytes, both stored base64 in the secret scope
  - the stored value keeps the 16-byte IV that Spark puts in front, so `aes_decrypt` needs no IV argument
  - no base64 line breaks (`base64_line_breaks: false`, pinned through `spark.sql.chunkBase64String.enabled`).

  All crypto SQL comes from `encrypt.CipherSpec`. It reads the key and IV with `secret()` inside the expression, so they never appear as literals in query plans.
- **What is encrypted.** All of this lives in `config/objects/<entity>.yml` (D-20):
  - `encrypted_fields`
  - `conditional_encryption`: Account `Name`, `FirstName`, `LastName` for person accounts, except one Id
  - `history_encryption`: AccountHistory `OldValue`/`NewValue` when `Field` is a listed PII field.
- **When.** Values are encrypted during the load, after hashing and before the MERGE, so plaintext exists only in the landing files. Logs carry counts and IDs, never values.
- **A field added to the PII config later.** Its existing values are encrypted in every version on the next load, exactly once: tracked in `ops_salesforce.encryption_state` and tagged in the Delta commit. A change to a `when` rule is not re-applied to existing rows.
- **Decrypt.** `ops_salesforce.decrypt_pii(value)` is a UC SQL function. Only `pii_readers_<catalog>`, which also needs READ on the secret scope, can execute it (FR-11).
- **Accepted trade-offs (design).** Deterministic CBC with a static IV reveals equal values and shares no integrity check. Plaintext exists briefly in landing files, readable by the service principal only.

## Authentication and network

- **JWT.** OAuth 2.0 JWT bearer flow against one Salesforce External Client App per org, through `simple-salesforce` (D-05, D-21). The private key secret holds either a PEM key, optionally followed by its certificate, or a base64 PFX with an optional password secret. The Bulk client reuses the access token and re-authenticates on 401.
- **Network.** Every call leaves through the workspace's NAT gateway (NFR-01). The integration user's profile only accepts that IP.

## Operations data (`ops_salesforce`)

| Table or view | Content |
| --- | --- |
| `run_events` | Append-only events per run, object and stage (`extract`, `load`, `deletes`, `ingest`, `backfill`), with counts, Bulk job IDs, watermarks and errors |
| `run_log` (view) | One row per run and object. Status is the latest event's (a repaired object reads `succeeded`); `error_message` is the last error |
| `watermark_history`, `watermarks` (view) | Append-only UTC watermarks; the view takes the maximum, so a watermark never moves back |
| `field_allowlist` | Fields per object with `allowed` 1/0 and their source (`seed`, `auto-add`, `approval`) |
| `field_approval` | New fields awaiting a decision (`pending` / `approved` / `rejected`) |
| `object_schema` | Latest describe output |
| `object_config` | Snapshot of the object config used by the latest preflight |
| `run_plan` | Fields queried per run and object |
| `encryption_state` | PII keys already encrypted per object |
| `view_state` | Hash of each view's SQL |
| `dq_results` | Every check result |
| `certificate` | JWT certificate expiry per run |
| `landing` (volume) | Raw pages during a run |

Worker threads only append to these tables, so parallel objects never conflict on a MERGE.

The ops dashboard (`dashboards/sf_ops.lvdash.json`) shows each run's finish time, duration, failed objects and the latest run per object.

## Initial load from `backup_salesforce`

The Synapse tables are deep-cloned by hand into `<catalog>.backup_salesforce`, one table (or view) per object named like its SCD2 table (`account`). `sf_seed_roojai` (`seed.py`) then, per object:
1. Reads the backup table and matches its columns to tonight's fields by name, case-insensitively. Fields missing in the backup load as null; an object without a backup table is skipped, and its first nightly run loads it in full.
2. Decrypts PII under each rule with the **legacy key** (`pii.legacy`: the Synapse and salesforce_dab key). Both stored forms are read: with or without the IV prefix, with or without line breaks. Values that aren't ciphertext are taken as plaintext. Ciphertext the legacy key can't read fails the object, with counts per column.
3. Casts to the column types. Backup timestamps are already in Bangkok time and are not shifted; a value that doesn't fit fails the object with counts.
4. Writes each record as its first SCD2 version (flags, hash of the plaintext, PII encrypted with the current key), or appends `*History` rows. It refuses to load into a non-empty table.
5. Seeds each watermark at the highest backup value minus 7 hours (back to UTC), and writes a `seed_count` check per object.

The first nightly run must follow within 15 days, while Salesforce's recycle bin still holds the deletes made in between.
