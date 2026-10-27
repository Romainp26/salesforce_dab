# Decision log

The design's 25 decisions (D-01 to D-25) are in `shared_folder/salesforce-ingestion-design.md` and hold unless an entry here supersedes them. This log records the bundle's own decisions (B-xx): what was decided, the alternatives and why they were set aside. Add new entries at the end; to revisit one, add a new entry that supersedes it rather than rewriting history.

```
## B-<n>: <title> (<YYYY-MM-DD>)
**Decision:** ...
**Alternatives:** ...
**Revisit when:** ...
```

## B-01: One ingest task instead of for-each extract and load tasks (2026-10-26)
**Default concurrency superseded by B-16 (4 at a time); B-21 adds a purge task before cleanup.**

**Decision:** The nightly job is `preflight → ingest → views → dq → cleanup`. The ingest task:
- runs every object in a thread pool of one Spark session, 8 at a time by default
- orders objects longest first by the last 14 days' durations; objects with no history go first
- gives each thread its own FAIR scheduler pool
- runs each object from extract to merge, then the hard-delete check for full loads, then deletes its landing files
- doesn't let a failed object stop the others, and fails at the end
- on repair, skips objects that already finished in the run.

Its output logs every object and ends with a summary table and the failures. This supersedes D-04's task layout (for-each extract, deletes task, for-each load), not its single cluster.

**Alternatives:**
- **Per-object for-each tasks (the design).** About 250 task starts a night, each paying its own start-up. Every object waits for all extracts before loading, and the night's story is split across 250 outputs. Per-object repair is kept anyway, by the skip on repair.
- **Batched for-each, with N buckets of objects.** Less overhead, but static buckets can't balance as well as a shared queue ordered longest first.

**Revisit when:** a single task's output or driver becomes a bottleneck, or per-object isolation in separate processes is needed.

## B-02: PII format follows salesforce_dab (2026-10-26)
**Superseded by B-13.**

**Decision:**
- the AES key secret is UTF-8 text and the IV secret is hex
- the stored value is `base64(aes_encrypt(...))` with Spark's 16-byte IV prefix kept (`iv_prefixed: true`)
- base64 line breaks follow Spark's default (CRLF every 76 characters), pinned by `pii.base64_line_breaks: true` through `spark.sql.chunkBase64String.enabled`.

The key and IV are still read with `secret()` inside SQL, so they never appear in query plans, which salesforce_dab didn't do. Matching salesforce_dab's expression byte for byte is tested.

**Alternatives:**
- **Strip the IV prefix,** the usual non-Spark format. It doesn't match the ciphertext salesforce_dab produced.
- **Leave line breaks to the platform default.** A runtime default change would silently change every new ciphertext and break joins with existing values.

**Revisit when:** the UAT check of stored values ([open-questions.md](open-questions.md) Q1) shows another format.

## B-03: Casts never throw (2026-10-26)
**Decision:** Casts use `try_cast`. Before merging, `profile_landed` counts per column the non-null values that don't fit their type and fails the object with those counts, never the values.

**Alternatives:**
- **ANSI casts.** The error message quotes the value, which can be PII, into logs and `run_log`.
- **Silent nulls.** Data loss nobody sees.

**Revisit when:** never, while landing files hold plaintext.

## B-04: Hard-deleted records that reappear become current; no snapshot floor (2026-10-26)
**Decision:** On full-load objects, a record flagged as hard-deleted that is back in a later extract gets a new current version that night, even if unchanged. Only an empty full extract is refused. There is no minimum share of records a full extract must hold.

**Alternatives:**
- **salesforce_dab's 50% snapshot floor.** Set aside by the user: SCD2 keeps every version, and with reappearance a bad night heals itself on the next run.
- **Leaving flags permanent (the design's literal reading).** A one-night visibility glitch would hide records for good.

**Revisit when:** a truncated extract proves costly before the next night repairs it.

## B-05: Per-environment pause (2026-10-26)
**Decision:** `paused: [uat|prod]` on an object makes the nightly run skip it in that environment, logged as `skipped`. Naming it in the `objects` parameter runs it anyway, for debugging. Contract and Payments__c are paused in UAT, where their Bulk queries fail with `INVALID_DATE_FORMAT` (found by salesforce_dab).

**Alternatives:** removing the objects from the config, which loses their settings and makes the failure invisible.

**Revisit when:** UAT data is fixed ([uat-checklist.md](uat-checklist.md) §2).

## B-06: Automatic REST fallback (2026-10-26)
**Decision:** When Bulk API 2.0 rejects an object as unsupported at job creation, the extract switches to REST `queryAll` and logs a warning to set `api: rest`.

**Alternatives:** failing the object until the config changes, which costs a night for a known, safe remedy.

**Revisit when:** a large object ever falls back, since REST is slow for millions of rows.

## B-07: Field backfill job (2026-10-26)
**Row hash: superseded by B-21** (the backfill rehashes the rows it updates).

**Decision:** `sf_backfill_<entity>`:
- runs `SELECT Id, <fields>` with no watermark, plus the fields a conditional encryption rule reads
- updates only the current versions; closed versions keep null
- never moves a watermark, and refuses `Id`, watermark columns and `*History` objects.

Re-encryption of newly listed PII runs first, as in the nightly load.

**Alternatives:**
- **A full reload.** Doesn't fill records whose watermark didn't change: an equal watermark is a replay and writes nothing.
- **Inventing historical values.** Unknowable.

**Revisit when:** consumers need the field on closed versions.

## B-08: The corrupted Contract record is filtered in SOQL (2026-10-26)
**Decision:** Contract carries `soql_filter: Id != '8002r000000vRMSAA2'`, added to every query of the object: nightly, `COUNT()`, full reloads and their ranges, REST and backfills. The record is corrupted, so any query returning it fails Salesforce-side.

The source CSV lists the condition under Contact, but Id prefix `800` is Contract's, so the generator maps it to Contract. Contact has no filter.

`soql_filter` exists only for records Salesforce can't return. Business exclusions stay in `view_filter` (D-17), so SCD2 remains a full copy.

**Alternatives:** a view filter (D-17), which is useless because the extract itself fails.

**Revisit when:** Salesforce fixes or deletes the record.

## B-09: Databricks Runtime 18 LTS and capped dependencies (2026-10-26)
**Cluster shape superseded by B-16.**

**Decision:**
- The job cluster runs `18.x-scala2.13` (Spark 4.1.0, Scala 2.13, Python 3.12); local tests use Spark 4.1 and Delta 4.3.
- Runtime dependencies are capped below their next major version (`simple-salesforce<2`, `requests<3`, `cryptography<51`, `pyyaml<7`), because the cluster resolves them from PyPI at every start.
- Python is pinned to 3.12.

**Alternatives:** uncapped dependencies, where a major release could break the nightly run with no change in the repository.

**Revisit when:** a new LTS, or a dependency's major release has been tested.

## B-10: Cleanup scope and run status (2026-10-26)
**Partly superseded by B-34:** preflight also deletes stale landing folders.

**Decision:**
- Cleanup deletes the run's own landing folders and any older than 12 hours, so a backfill running alongside keeps its files.
- `run_log.status` is the latest event's status, so an object that failed and then succeeded in a repair reads `succeeded`; `error_message` keeps the last error.

**Alternatives:**
- **Wiping the whole volume.** Deletes another job's in-flight files.
- **"Failed if any event failed".** Shows repaired objects as failed forever.

**Revisit when:** jobs of one catalog run concurrently for longer than 12 hours.

## B-11: Retry dropped page downloads by locator (2026-10-26)
**Decision:** A Bulk result page whose stream drops (connection reset, chunked-encoding error, read timeout) is fetched again by its locator into the same file, up to 5 attempts with backoff.

**Alternatives:** failing the object, which costs a repair for a transient network error.

**Revisit when:** never.

## B-12: No named SQL parameters (2026-10-26)
**Decision:** The code doesn't use `spark.sql(..., args=...)`. Reads filter with the DataFrame API; the few SQL statements left (`DELETE`, the view-state `MERGE`) embed values through `ops.sql_string`. With Spark 4.1 and Delta 4.3, named parameters came back unbound in Delta statements and in queries over Delta tables and views, failing 16 tests after the upgrade to the DBR 18 line (B-09).

**Alternatives:** keeping parameters and trusting DBR 18's own Delta to bind them. That is unverifiable locally, and the values involved (run IDs, object names, hashes) come from the job and the config, not from users.

**Revisit when:** never, unless user-supplied values ever reach SQL.

## B-13: A new PII key in Spark's native format (2026-10-27)
**Decision:** Encrypted fields no longer need to match salesforce_dab's ciphertext: the seed re-encrypts the backup, and the backfill job can re-ingest any encrypted field. So PII uses a new key:
- 32 random bytes (AES-256) and a 16-byte random static IV, both stored base64 in the environment's secret scope
- `base64(aes_encrypt(value, key, 'CBC', 'PKCS', iv))`, keeping the IV prefix Spark writes, so `aes_decrypt` needs no IV argument
- no base64 line breaks, pinned.

The old key stays configured as `pii.legacy`, read only by the seed to decrypt `backup_salesforce`. This supersedes B-02 and the design's reuse of the Synapse key (D-07).

**Alternatives:**
- **Keep the legacy key and format.** Its key is UTF-8 text rather than random bytes. salesforce_dab put it as a literal in query plans (its Q19), so it may be exposed in Spark UIs and logs. And its stored form varies (line breaks, IV prefix).
- **AES-SIV** (RFC 5297), the textbook deterministic authenticated encryption. Spark has no built-in for it: encryption would need a Python UDF and decryption a Python UC function for BI tools, with the key passed in from `secret()`. That's a large step in complexity and cost for the gain.
- **CBC with a synthetic IV per value** (IV = keyed hash of the plaintext), which hides shared 16-byte prefixes. Spark SQL has no HMAC, so this would mean hand-built crypto.

Deterministic CBC with a static IV keeps the design's accepted trade-offs: equal values and equal 16-byte prefixes are visible, and there is no integrity check.

**Revisit when:** Databricks ships a deterministic AEAD function, or the threat model changes.

## B-14: Seed from `backup_salesforce` replaces the Synapse migration job (2026-10-27)
**Decision:** The Synapse tables are deep-cloned by hand into `<catalog>.backup_salesforce`. A one-off job `sf_seed_<entity>` (preflight → seed → views) then, per object:
- matches backup columns to Salesforce fields by name
- decrypts PII with the legacy key (both stored forms) and re-encrypts it with the current key
- casts, flags and hashes, and writes first versions to empty SCD2 tables
- seeds the watermarks, and checks counts.

The JDBC migration job, its mapping file and the ciphertext spike notebook are removed. Objects without a backup table load in full on their first nightly run.

**Alternatives:**
- **The JDBC job.** Unneeded: the copy out of Synapse happens by hand.
- **Seeding with hand-written SQL in the runbook.** 124 objects, each needing types, flags, hash, decryption, encryption and watermarks done identically to the nightly load. The code does it the same way every time, with tests.
- **Seeding automatically in the nightly run when a table is missing.** A reset (runbook §5.9) would then reload stale backup data.

**Confirmed (2026-10-27):**
- Synapse tables and columns use the Salesforce API names, so a plain deep clone matches the seed's name matching.
- Synapse timestamps hold Bangkok time, so the seed loads them unshifted.
- The platform team will create the new key and IV and grant read access to the legacy key's scope (former Q11 and Q12).

**Revisit when:** a second entity (Kumka) has its own legacy store to load.

## B-15: Views carry business columns only (2026-10-27)
**Decision:** `bronze_salesforce` views select the Salesforce fields only; `_is_deleted`, `_deleted_at`, `_is_archived`, `_archived_at` and `_valid_from` stay in `bronze_salesforce_scd2`. In `retain` views, soft-deleted and archived records show with Salesforce's `IsDeleted = true`, and `LastModifiedById` identifies archivals. This supersedes the design's flagged views.

**Alternatives:** keeping the flags, which puts non-Salesforce columns in the consumer-facing views.

**Revisit when:** consumers need to tell archivals from soft deletes without knowing the archival users.

## B-16: Single-node Photon cluster, Salesforce API v67 (2026-10-27)
**Decision:**
- The job cluster is `18.x-photon-scala2.13` on one `Standard_D4ds_v5` (4 vCPU, 16 GB) in single-node mode.
- The ingest task runs 4 objects at a time by default, one per core.
- Salesforce calls use API version 67.0.

**Alternatives:**
- **Multi-node autoscaling (2–8 × D8ds_v5).** More capacity than a nightly job with no deadline needs.
- **Without Photon.** Slower MERGEs.

**Revisit when:** a night runs too long or the node runs short of memory on the largest objects (Q6).

## B-17: Predictive optimization instead of a maintenance job (2026-10-27)
**Partly superseded by B-21:** the purge task runs REORG and VACUUM on tables whose PII was rewritten in place.

**Decision:** Preflight enables predictive optimization on `bronze_salesforce_scd2` and `ops_salesforce`, and deletes `run_plan` rows older than 30 days. The weekly `OPTIMIZE`/`VACUUM` job is removed.

**Alternatives:** the weekly job, which needs a schedule, a cluster and tuning that predictive optimization handles on its own.

**Revisit when:** predictive optimization isn't available on the account, or its costs need control.

## B-18: PII objects require approval; delete-detection fields always queried (2026-10-27)
**Decision:**
- Config validation refuses an object with `encrypted_fields` or `conditional_encryption` unless it has `new_fields: require-approval`. A new field on an object that holds PII is never stored before someone decides whether it is PII. This was already true of the config and is now enforced.
- `IsDeleted` and `LastModifiedById` are always queried when the object has them, whatever the allowlist says, because soft-delete and archival detection read them.

**Alternatives:**
- **Trusting the config.** A future edit could silently allow unencrypted PII.
- **Relying on the allowlist for the system fields.** Excluding one by mistake would silently stop delete detection.

**Revisit when:** never.

## B-19: One object never fails preflight (2026-10-27)
**Partly superseded by B-23:** in PROD an unpaused missing object is failed, not skipped.

**Decision:** Preflight describes and plans each object on its own:
- an object whose `describe` returns 404 (not in this org) gets no plan and a `preflight` / `skipped` event
- any other describe error, or a plan that lacks `Id` or the watermark column, gives a `preflight` / `failed` event with the error
- both raise an `object_planned` warning, except a missing object paused in that environment (pausing it acknowledges the gap)
- the object keeps its previous `object_schema` snapshot, so removed and retyped fields are still compared once it is described again.

Ingest reports these objects without running them. A failed one counts as a failed object: the other objects finish, then the task fails and the job alerts. Setup and authentication errors still fail preflight, since no object could run.

**Alternatives:**
- **Preflight fails on any object (the previous behaviour).** One object missing from a UAT sandbox, or one Salesforce 5xx, stopped all 124 objects for the night.
- **Skip every unplannable object silently, as salesforce_dab's drift report did for missing objects.** A describe error or a plan error is not drift. Counting it as a failure keeps it visible in the job status.

**Revisit when:** describe errors turn out to be transient often enough to need retries inside preflight.

## B-20: Task timeouts and a maximum Bulk job wait (2026-10-27)
**Extended by B-27:** the rotate task also uses `long_task_timeout_seconds`.

**Decision:**
- Every task has `timeout_seconds`. Ingest, backfill and seed use `long_task_timeout_seconds` (6 hours by default); the other tasks have 1 hour.
- The timeouts are per task, not per job. A job-level timeout cancels the whole run, so `cleanup` would never run and plaintext landing files would stay until the next run. A timed-out task is done, so the `run_if: ALL_DONE` tasks after it still run.
- The Bulk client aborts a query job still running after 3 hours (`bulk.MAX_JOB_WAIT_SECONDS`) and fails that object. One stuck job no longer holds the ingest task until its timeout.

Without them, a stuck job polled forever and, with `max_concurrent_runs: 1`, blocked every later night.

**Alternatives:**
- **A job-level timeout only.** Simpler, but it skips cleanup, as above.
- **No Bulk wait limit, the task timeout alone.** A stuck job would then fail every object still running at the timeout, not just its own.

**Revisit when:** a normal night or a first full load comes close to a timeout (UAT checklist §5).

## B-21: PII config changes reach every version and leave no recoverable plaintext (2026-10-27)
Follows the audit of 2026-10-26 ([audit-2026-10-26.md](audit-2026-10-26.md), F-01, F-02, F-03, F-14).

**Decision:**
- **Row hash over stored values.** `_row_hash` is the SHA-256 of the row as stored (typed values, PII as ciphertext), no longer of the landed plaintext. It reveals nothing the row doesn't, so it can't be brute-forced into a PII value. Tables hashed the older way (no `sf_ingest.row_hash = stored-v2` property) are rehashed once. Every in-place rewrite of a PII column rehashes the rows it rewrites in the same statement.
- **History objects inherit their parent's PII.** A `*History` object encrypts `OldValue`/`NewValue` for every field in its parent's `encrypted_fields` and `conditional_encryption` columns, on top of its own `history_encryption`. Parents: `XHistory` → `X`, `X__History` → `X__c`, `OpportunityFieldHistory` → `Opportunity`. History objects without `OldValue`/`NewValue` (OpportunityHistory) are left alone.
- **Applied every night.** PII config changes are applied to an existing table even when the object extracted nothing (`scd2.prepare_existing`), not only on its next change.
- **Purged.** Every in-place rewrite (encryption, decryption, retype, rehash, rotation) queues a purge in `ops_salesforce.pii_purge`. The nightly `purge` task, after `dq`:
  1. runs `REORG TABLE … APPLY (PURGE)` (rewrites the files whose plaintext rows a deletion vector only masks), records the data files of the version before the rewrite that the table no longer uses, and sets a 12-hour file retention, a one-day log retention and a checkpoint at every commit;
  2. the next night, 12 hours later at least, runs `VACUUM` and checks with `VACUUM … DRY RUN` that none of the recorded files is left (`pii_history_purged`, critical). Checking that the old version can't be read isn't enough: Databricks refuses time travel beyond the file retention whether the files are gone or not;
  3. on later nights, commits once so the checkpoint deletes the expired log (which holds per-file min/max statistics of the old files), and restores the default retentions once the history starts after the purge (`pii_log_purged`, warning after 4 days).

**Alternatives:**
- **A keyed hash** (`sha2(secret pepper || row)`). Also safe, but needs a new secret, and still hashes plaintext.
- **Purging by hand from the runbook.** Every policy change would depend on someone remembering it; the plaintext stays recoverable meanwhile.
- **Lowering retention for good.** No time travel or `RESTORE` on any table, all the time.
- **`VACUUM … RETAIN 0 HOURS` on the first night** (the first version of this decision). Faster, but it deletes files a concurrent writer hasn't committed yet, which corrupts the table; the backfill and seed jobs can run alongside.

**Revisit when:** Delta can purge a column's history in one step, or the retention needs of `RESTORE` change.

## B-22: Separate secret scopes for Salesforce and for the PII key (2026-10-27)
**Decision:** `environments.<env>.secret_scope` holds the Salesforce JWT secrets and is readable by the service principal only. Each entity's AES key and IV live in `entities.<entity>.pii_secret_scopes.<env>`, readable by the service principal and `pii_readers_<catalog>`. Config validation refuses the same scope for both. This supersedes the design's single scope and its grant of READ on it to `pii_readers` (audit F-04).

**Alternatives:** one scope with per-secret ACLs, which Databricks secret scopes don't have.

**Revisit when:** `secret()` inside a UC function no longer needs the caller's READ on the scope (Q4).

## B-23: A missing object fails in PROD; per-object freshness (2026-10-27)
**Decision:**
- A `describe` 404 means `skipped` in UAT, and in any environment where the object is paused. In PROD an unpaused missing object is `failed`, so the job alerts: a 404 is also what a lost object permission looks like.
- The `dq` task raises `object_freshness` (warning) for every unpaused, unretired object without a successful ingest for `freshness_hours` (36).

**Alternatives:** keep "skipped" everywhere (B-19), which leaves a PROD object silently stale behind a green job.

**Revisit when:** UAT needs the same strictness.

## B-24: Decrypting or retyping PII columns (2026-10-27)
**Decision:**
- **Removing a field from the PII config** fails the object unless the field is listed in the object's `decrypt_fields`. Then every version is decrypted in place, and the column goes back to its describe type when that isn't STRING. `ops_salesforce.encryption_state` is append-only and records `decrypted`, so listing the field again later encrypts every version again.
- **Encrypting a non-STRING column** (a date, a number) rewrites the table with `CREATE OR REPLACE TABLE … AS SELECT`, the column as STRING ciphertext. The plaintext encrypted is the text Bulk API CSV gives for the value (`encrypt.raw_text_sql`), so values re-encrypted in place match the ciphertext of later loads. This is exact for dates, datetimes, booleans and integers; decimals drop trailing zeros (Q12).
- Commit tags carry a sequence number per key, so a key encrypted, decrypted and encrypted again is tagged anew each time.

**Alternatives:** a reset (runbook §5.9), which drops the SCD2 history.

**Revisit when:** a numeric PII field turns out to land in a format `raw_text_sql` doesn't rebuild (Q12).

## B-25: Hard-delete check of incremental objects (2026-10-27)
**Decision:** Every `reconcile.every_days` (7) nights, each incremental, non-History object gets an Id-only `queryAll` extract after its load. It includes the recycle bin, so a missing Id was hard-deleted or purged.
- Current records missing from it get a version flagged `_is_deleted` and `_is_hard_deleted`, and every view hides them.
- Flagged records that are back are restored.
- A check that would flag more than `reconcile.max_deleted_share` (5 %) of the current records, and more than 10 records (`deletes.SHARE_GUARD_FLOOR`), fails the object instead. An empty Id extract fails it too while the table has current records, unless `max_deleted_share` is 1.
- Each object's first check falls on a night set by a hash of its name, so the checks spread over the week.

This supersedes D-18's "not checked for" (audit F-05). Full-load objects keep their nightly check, now also setting `_is_hard_deleted`.

**Alternatives:** a weekly separate job, which would compete with the nightly job for the same tables.

**Revisit when:** the archival process turns out to hard-delete (Q13): those records would be hidden rather than shown as archived.

## B-26: PII config is checked against Salesforce and against the pipeline (2026-10-27)
**Decision:**
- Preflight fails an object whose `encrypted_fields` or conditional columns aren't in Salesforce's describe: a typo, an undeployed field or field-level security would otherwise store the real column in plaintext.
- Config validation refuses encrypting `Id`, the watermark, `CreatedDate`, `IsDeleted`, `LastModifiedById` and any column the `conditional_encryption` rule reads.
- `encrypt_columns` evaluates the rule before encrypting anything.

**Alternatives:** relying on the static test against the seed CSV, which doesn't see runtime approvals or Salesforce.

**Revisit when:** never.

## B-27: PII key rotation (2026-10-27)
**Decision:** A new key gets a new `pii.key_version` and new secret names; the old ones move to `pii.previous_keys`. The on-demand job `sf_rotate_<entity>` re-encrypts every PII value of every table from each older version to the current one. It updates `_pii_key_version` and the row hash in the same statement, records the new version in `encryption_state` and queues a purge. Until it has run, the nightly load refuses every object whose state still names an older key. Rows stamped with a key version no longer in `previous_keys` but holding no ciphertext (written before the object had PII) are restamped, not refused. Objects removed from the config are rotated too, by their table name. This answers Q7.

**Alternatives:** the runbook's hand-written UPDATE, which left the old key's ciphertext in the history and the hashes stale.

Consumers decrypt during a rotation with `ops_salesforce.decrypt_pii_at(value, _pii_key_version)` over the SCD2 tables (the views don't carry `_pii_key_version`), which picks the key of the row's version; `decrypt_pii` knows only the current key.

**Revisit when:** never.

## B-28: Field approvals are config, reviewed by pull request (2026-10-27)
**Decision:** Decisions on new fields live in `config/approvals/<entity>.yml` (`object`, `field`, `decision`, `pii`, `decided_by`, `decided_on`). Preflight applies them to `ops_salesforce.field_allowlist` in every environment and answers the matching `pending` rows of `field_approval`. An approval must say `pii: true` or `pii: false`, and an approved PII field must already be encrypted: config validation refuses it otherwise. This replaces approving by updating `field_approval`, which was applied per catalog (UAT approvals never reached PROD) and separately from `encrypted_fields` (audit F-16, F-25, F-30).

**Alternatives:** a `pii` flag on `field_approval`, which keeps the per-catalog divergence.

**Revisit when:** approvals become frequent enough that a pull request per decision is too slow.

## B-29: Access as code (2026-10-27)
**Decision:**
- Preflight grants USE and SELECT on `bronze_salesforce` to `sf_readers_<catalog>`, on `bronze_salesforce_scd2` to `sf_history_readers_<catalog>`, and on `ops_salesforce` to `sf_operators_<catalog>`. It also grants EXECUTE on `decrypt_pii` (group names in `entities.yml` `access`). Nobody gets write access to the ops tables: field approvals and exclusions are config (B-28), so a hand-written `field_allowlist` row can't allow an unencrypted PII field. The landing volume is never granted.
- Every job gives `sf_operators_<catalog>` CAN_MANAGE_RUN.
- `workspace.root_path` is in the service principal's folder whoever deploys, so only CI deploying as that principal (and workspace admins) can change the deployed bundle.
- The wheel carries `config/` (`sf_ingest/_config`) and the jobs read it there, not from workspace files, so the config always matches the code reviewed and deployed with it.

**Alternatives:** grants by hand per the runbook, which nothing checks.

**Revisit when:** the platform manages UC grants elsewhere (Terraform).

## B-30: Retired and orphaned objects (2026-10-27)
**Decision:** `retired: true` keeps an object in the config but stops describing and loading it, drops its view and comments its SCD2 table. Preflight raises `object_orphaned` for SCD2 tables and views of objects missing from the config, whose data is frozen.

**Alternatives:** deleting the object from the YAML (runbook §5.7, before), which left the view serving frozen data silently.

**Revisit when:** never.

## B-31: Account's formula phones are PII (2026-10-27)
**Decision:** `Mobile_Formula__pc` and `Phone_Formula__pc` join Account's `encrypted_fields`: they copy the person-account phones. The data owner reviewed the audit's other candidates (F-09). Lead stays `auto-add` without PII, and the Contact, Case and Claim__c lists stay as they are. The design's 29 encrypted fields are now 31.

**Alternatives:** none: the formula fields expose the phones otherwise.

**Revisit when:** the classification is reviewed again.

## B-32: Schema drift never changes values silently (2026-10-27)
**Decision:**
- Retype detection compares precision and scale of numeric fields, not only the type.
- A load fits the stored column to the field's current type. It widens a decimal (more digits or decimals) or an integer with Delta type widening, keeps a type that already holds the values, and fails the object on any other change (datetime ↔ date, fewer integer digits).
- A value that a decimal cast would round counts as not fitting.
- An allowed field absent from Salesforce's describe is reported every night (`field_removed`), the first run included, until it is described again or excluded with a `rejected` approval (`config/approvals`). A configured PII field absent from describe fails the object (B-26).

**Alternatives:** keep the old column type, as before, which rounded new decimals and shifted dates silently.

**Revisit when:** Delta type widening covers date → timestamp.

## B-33: Corrective full reload (2026-10-27)
**Decision:** The nightly `rewrite` parameter extracts the named objects in full and writes a correction version, starting tonight, for every record whose stored values differ from Salesforce's at an unchanged watermark (formula drift, D-25; a past bad load). `full_reload` still writes only records whose watermark changed.

**Alternatives:** a reset, which drops the SCD2 history.

**Revisit when:** formula drift needs regular correction (D-25).

## B-34: Operational safeguards (2026-10-27)
**Decision:**
- `schedule_pause_status` is PAUSED by default and per target, until the environment is seeded.
- An object whose first load would run while `backup_salesforce` holds an unseeded table for it fails instead.
- Preflight deletes stale landing folders, so a cancelled run's plaintext files go at the next run of any job.
- The Bulk client retries the job-creating POST only on a connect timeout or 429, never on a read timeout or 5xx, which may already have created the job.
- The bundle requires CLI 0.299 or later (`dataset_catalog` / `dataset_schema` on the dashboard).

**Alternatives:** relying on the runbook's order of steps.

**Revisit when:** never.

## B-35: Bulk downloads are checked against the job's count (2026-10-27)
**Decision:** A Bulk query whose downloaded records differ from the job's `numberRecordsProcessed` fails the object before the load, so the watermark never advances past lost records. The `COUNT()` comparison stays a warning: it races with changes made during the extract.

**Alternatives:** a critical `COUNT()` check, which fails on legitimate changes.

**Revisit when:** the REST path (two small objects) needs the same check.

## B-36: Views are complete until the last successful load (2026-10-27)
**Decision:** With `include_created_today: false`, a view shows records created before the Bangkok date on which the object's latest successful ingest started (today before the first one), not before the reader's today. After a failed night the view still stops at the end of a complete day.

**Alternatives:** the reader's date (before), which showed a partial day after a failed night.

**Revisit when:** consumers need a freshness column instead.

## B-37: Data quality checks scale with the night, not the history (2026-10-27)
**Decision:** `one_current_row_per_id` and `pii_is_ciphertext` check the rows written by the run. Every row is checked on Sundays (Bangkok), with the nightly `dq_full` parameter, and for objects whose PII was rewritten in place that night.

**Alternatives:** full scans every night, whose cost grows with every version kept.

**Revisit when:** a defect that only a full scan catches goes unnoticed for a week.

## B-38: The seed hashes and normalises like the nightly load (2026-10-27)
**Decision:** The seed hashes the stored values with the same function as the nightly load, and turns empty strings into null as Bulk CSV does. An unchanged seeded record therefore never gets a spurious version, and an empty PII value stays null instead of becoming ciphertext.

**Alternatives:** none.

**Revisit when:** never.
