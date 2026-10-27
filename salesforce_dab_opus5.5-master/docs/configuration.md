# Configuration reference

Every setting the solution reads, where it lives and what it does. Config files are packaged in the wheel (`sf_ingest/_config`) and the jobs read them there (B-29), so a change takes effect at the next deploy. Values marked `TODO` must be filled in before the first deploy.

## `databricks.yml`: bundle, targets and variables

| Item | Meaning |
| --- | --- |
| Targets `uat`, `prod` | Both `mode: production` in one shared workspace (`workspace.host`). Each runs as its own service principal (`run_as`) and deploys into that principal's folder (`workspace.root_path`, B-29) |
| `bundle.databricks_cli_version` | `>= 0.299.0`, for the dashboard's `dataset_catalog` / `dataset_schema` (B-34) |
| `service_principal_app_id` | Application ID of the target's service principal. Jobs run as it and it owns the catalogs' objects |
| `cluster_policy_id` | Cluster policy pinning local disk encryption and single-user mode |
| `alert_email` | Job-failure emails and the daily warnings alert |
| `data_owner_email` | Daily digest of new Salesforce fields |
| `warehouse_id` | SQL warehouse for the alerts and the dashboard |
| `schedule_pause_status` | `UNPAUSED` or `PAUSED` for the nightly schedule. `PAUSED` by default and in both targets; set the target's value to `UNPAUSED` once its catalog is seeded (B-34) |
| `ingest_workers` | Default number of objects the ingest task runs at a time (4, the node's cores) |
| `long_task_timeout_seconds` | Timeout of the ingest, backfill, seed and rotate tasks (21600, 6 hours). The other tasks have 1 hour. Per task, so the tasks after a timed-out one still run (B-20) |
| `sf_cluster` | The classic job cluster every task uses: DBR `18.x-photon-scala2.13` (Spark 4.1, Python 3.12, Photon), single node `Standard_D4ds_v5` (4 vCPU, 16 GB), `SINGLE_USER` as the service principal, local disk encryption, session time zone UTC, FAIR scheduler |

`sync.exclude` keeps `shared_folder/`, `tests/` and `scripts/` out of the workspace.

## `config/entities.yml`: orgs, secrets and keys

| Key | Meaning |
| --- | --- |
| `api_version` | Salesforce API version for every call (`67.0`) |
| `bulk_page_size` | Records per Bulk API result page, so per landing file (100,000) |
| `full_reload_split_from` | First year of the yearly `CreatedDate` ranges of a split full reload |
| `freshness_hours` | Hours without a successful ingest before `object_freshness` warns (36, B-23) |
| `reconcile.every_days` | Nights between hard-delete checks of an incremental object (7; 0 turns them off, B-25) |
| `reconcile.max_deleted_share` | Share of current records a hard-delete check may flag before it fails instead (0.05), once more than 10 records would be flagged. 1 accepts an empty Id extract (every record really deleted), for one run |
| `access.readers`, `access.history_readers`, `access.operators`, `access.pii_readers` | Groups preflight grants to, `{catalog}` replaced: views; SCD2 tables; `ops_salesforce`; EXECUTE on `decrypt_pii` and `decrypt_pii_at` (B-29). Nobody gets write access to `field_allowlist` |
| `pii.key_version` | Version written to `_pii_key_version` |
| `pii.key_encoding`, `pii.iv_encoding` | How the key and IV secrets are stored: `base64` (current), `hex` or `utf8` |
| `pii.iv_prefixed` | Keep the 16-byte IV that Spark prepends to the ciphertext (`true`, as stored today) |
| `pii.base64_line_breaks` | Spark's CRLF every 76 base64 characters: `false`. Pinned so a platform default can't change ciphertext |
| `pii.aes_key_secret`, `pii.aes_iv_secret` | Secret names of the AES key and IV (`sf-pii-aes-key`, `sf-pii-aes-iv`), in the entity's PII scope. How to generate them is in `entities.yml` |
| `pii.previous_keys` | Keys a rotation replaced: `key_version`, `aes_key_secret`, `aes_iv_secret`, read only by the rotate task (B-27) |
| `pii.legacy` | The key of the `backup_salesforce` data, read only by the seed: `secret_scope` (default: the entity's PII scope), `key_encoding` (`utf8`), `iv_encoding` (`hex`), `aes_key_secret`, `aes_iv_secret`. Remove once every environment is seeded |
| `environments.<env>.secret_scope` | The environment's Key Vault-backed scope for the Salesforce JWT secrets: READ for the service principal only (B-22) |
| `entities.<entity>.pii_secret_scopes.<env>` | The entity's scope for the AES key and IV: READ for the service principal and `pii_readers_<catalog>`. Must differ from the Salesforce scope |
| `entities.<entity>.country` | Catalog name part: catalog = `<entity>_<country>_<env>` |
| `entities.<entity>.archival_user_ids` | Users whose deletions are archivals (D-23) |
| `entities.<entity>.secrets` | Secret names of the External Client App's consumer key, the JWT private key (PEM, optionally followed by its certificate, or base64 PFX) and the optional PFX password |
| `entities.<entity>.orgs.<env>.domain` | `login`, `test`, or a My Domain prefix such as `roojai--uat.sandbox.my` |
| `entities.<entity>.orgs.<env>.username` | The integration user |

Secrets expected:
- in the environment's Salesforce scope: the consumer key and the private key, plus the password if the key is a protected PFX
- in the entity's PII scope: the AES key and IV, and the previous keys during a rotation
- for the seed, the legacy AES key and IV (`pii.legacy`), possibly in another scope.

## `config/objects/<entity>.yml`: objects and their policies

`defaults` apply to every object; each entry lists only what differs. Changes go through pull requests.

| Key | Values (default first) | Effect |
| --- | --- | --- |
| `name` | Salesforce API name | Table and view name are its lower case |
| `watermark` | `SystemModstamp`, `CreatedDate` | Incremental column. `*History` objects use `CreatedDate` |
| `api` | `bulk`, `rest` | `rest` for objects the Bulk API can't query (ContractStatus, TaskStatus) |
| `load_type` | `incremental`, `full` | `full` extracts every record nightly and flags missing ones as hard-deleted (User, GroupMember) |
| `delete_policy` | `retain`, `delete` | Whether the view shows soft-deleted records, flagged, or hides them |
| `include_created_today` | `false`, `true` | `false` hides records created on or after the Bangkok date on which the object's latest successful ingest started (today before the night's ingest; also yesterday's after a failed night), B-36 |
| `view_filter` | SQL condition | Extra view condition, for business exclusions (D-17). None configured |
| `soql_filter` | SOQL condition | Added to every query of the object. Only for records Salesforce can't return: Contract `Id != '8002r000000vRMSAA2'` (B-08) |
| `new_fields` | `auto-add`, `require-approval` | New Salesforce fields: queried immediately, or held for approval. Must be `require-approval` on objects with `encrypted_fields` or `conditional_encryption` |
| `encrypted_fields` | list | Columns always encrypted. Never `Id`, the watermark, `CreatedDate`, `IsDeleted`, `LastModifiedById`, nor a column the `when` rule reads (B-26) |
| `conditional_encryption` | `columns`, `when` (SQL) | Columns encrypted where `when` is true (Account names) |
| `history_encryption` | `fields` | `*History` only: encrypt `OldValue`/`NewValue` when `Field` is listed (AccountHistory). The parent object's PII fields are added automatically (B-21) |
| `decrypt_fields` | list | Fields removed from the PII config whose existing ciphertext is decrypted in every version (B-24). Without it, removing a PII field fails the object |
| `hash_exclude` | list | Fields whose changes don't make a new version. Default `SystemModstamp`, `LastViewedDate`, `LastReferencedDate` |
| `split_full_reload` | `false`, `true` | A full reload runs yearly `CreatedDate` ranges in parallel. Set it on the largest objects |
| `paused` | list of `uat`, `prod` | Environments where the nightly run skips the object; naming it in `objects` still runs it. A missing object paused in an environment is skipped there without a warning |
| `retired` | `false`, `true` | No longer described or loaded; its view is dropped and its SCD2 table kept (B-30) |

Unknown keys, unknown values and duplicate objects fail at load time. Roojai has 124 objects; `tests/test_config.py` pins its counts against the design, plus the two encrypted fields of B-31.

`scripts/generate_config.py` bootstrapped this file once from `shared_folder/objects_allowlist_and_watermarks.csv` and `encrypted_fields.txt`. It refuses to overwrite without `--force`; edit the YAML directly from now on.

## `config/approvals/<entity>.yml`

Decisions on new fields (B-28), one entry each: `object`, `field`, `decision` (`approved` or `rejected`), `pii` (`true` or `false`, required for an approval), `decided_by`, `decided_on`. Preflight applies them to `ops_salesforce.field_allowlist` in every environment and marks the pending rows of `field_approval`. An approved field with `pii: true` must already be in the object's `encrypted_fields` or conditional columns, else config validation fails.

## `config/seed/<entity>_fields_allowlist.csv`

`object,field,allowed` rows (`1` queried, `0` deliberately excluded and never reported). Preflight inserts rows missing from `ops_salesforce.field_allowlist` and never overwrites existing ones, so run-time approvals and auto-adds win. To change an existing field's status, add a decision to `config/approvals/<entity>.yml` (runbook §5.5), not the seed: nobody has write access to the table.

## Job parameters

| Job | Parameter | Meaning |
| --- | --- | --- |
| nightly | `objects` | Comma-separated subset; empty runs all. A paused object named here runs |
| nightly | `full_reload` | Comma-separated objects to extract in full this run |
| nightly | `workers` | Objects at a time (default `ingest_workers`) |
| nightly | `rewrite` | Comma-separated objects to extract in full and correct where stored values differ at an unchanged watermark (B-33) |
| nightly | `dq_full` | `true` checks every row tonight; Sundays always do (B-37) |
| backfill | `fields` | Comma-separated `Object.Field` entries |
| seed | `objects` | Comma-separated subset; empty seeds every object that has a backup table |
| rotate | `objects` | Comma-separated subset, to rerun the objects that failed; empty rotates every object with encrypted values |

## Code constants

Change these in code, with tests:

| Constant | Value | Where |
| --- | --- | --- |
| Watermark overlap | 1 hour | `extract.OVERLAP` |
| Parallel ranges of a split full reload | 4 | `extract.SPLIT_WORKERS` |
| Bulk request attempts, backoff, polling | 5 attempts, 5 s doubling; poll 5 s × 1.5 up to 60 s | `bulk.BulkClient` |
| Longest wait for one Bulk query job, then aborted | 3 hours | `bulk.MAX_JOB_WAIT_SECONDS` |
| Default ingest workers | 4 | `tasks.DEFAULT_WORKERS` |
| Stale landing folder age | 12 hours | `tasks.STALE_LANDING` |
| Table properties during a purge | 12-hour file retention, one-day log retention, a checkpoint per commit | `tasks.PURGE_FILE_RETENTION`, `tasks.purge_properties` |
| Purge log cleanup warning | 4 days | `tasks.PURGE_LOG_WARNING` |
| Certificate warning | 30 days | `tasks.CERT_WARNING_DAYS` |
| Duration history for ordering | 14 days | `ops.recent_durations` |
| Skipped field types | `address`, `location`, `base64` | `metadata.SKIP_TYPES` |
| Always-queried fields | `IsDeleted`, `LastModifiedById` | `metadata.SYSTEM_FIELDS` |
| Backup schema | `backup_salesforce` | `config.BACKUP_SCHEMA` |
| Bangkok offset | +7 hours | `transform.BANGKOK_OFFSET_HOURS` |
