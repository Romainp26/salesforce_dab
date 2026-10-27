# Requirements traceability

Each requirement of the design (`shared_folder/salesforce-ingestion-design.md`, confirmed 25 Oct 2026), where it is implemented and how it is verified.

Status:
- **Implemented**: built and covered by tests
- **Partly**: built, with the gap named
- **Platform**: outside this repository (platform team or Salesforce admins)

## Functional

| ID | Requirement | Status | Implemented in | Verified by |
| --- | --- | --- | --- | --- |
| FR-01 | Ingest standard and custom objects from the Roojai and Kumka orgs | Partly: Roojai configured; Kumka needs its lists (D-22) | `config/objects/roojai.yml`, `config/entities.yml` | `test_config.py` |
| FR-02 | Object list, fields and policies in configuration, not code | Implemented | `config/objects/<entity>.yml`, `config/seed/`, `config.py` | `test_config.py` |
| FR-03 | Load changed records incrementally once per night | Implemented; Bulk downloads checked against the job's count (B-35) | `extract.build_where` (watermark − 1 h overlap), `tasks.ingest_object`, `bulk.BulkClient.query_to_files` | `test_extract.py`, `test_tasks.py`, `test_bulk.py` |
| FR-04 | Migrate Synapse as the initial load; on-demand full reload per object | Implemented: seed from the manual clone in `backup_salesforce` (B-14) | `seed.py`, `sf_seed` job; `full_reload` parameter | `test_seed.py`, `test_tasks.py` |
| FR-05 | Keep every version as SCD Type 2 | Implemented | `scd2.load_object`, `changed_rows`, `merge_versions` | `test_scd2.py` |
| FR-06 | Delete policy in the view: show flagged or hide | Implemented; flagged through Salesforce's `IsDeleted` (B-15); hard deletes hidden on every object (B-25) | `views.view_sql`, `scd2.delete_flags`, `deletes.flag_hard_deletes` | `test_deletes_views_dq.py`, `test_tasks.py` |
| FR-07 | Created-today policy in the view | Implemented; relative to the last successful ingest (B-36) | `views.view_sql`, `views.complete_until` | `test_deletes_views_dq.py` |
| FR-08 | `*History` objects as append-only field changes | Implemented | `scd2.load_object` (history branch) | `test_scd2.py::test_history_is_append_only_shifted_and_encrypted` |
| FR-09 | New fields auto or after approval; alert on removed or retyped | Implemented; approval enforced on PII objects (B-18), decided in config (B-28); retypes include precision and scale, widened or failed (B-32) | `metadata.plan_fields`, `tasks.preflight`, `tasks.apply_approvals`, `scd2.evolve_types`, SQL alerts | `test_metadata.py`, `test_tasks.py`, `test_scd2.py`, `test_config.py` |
| FR-10 | Reversible deterministic PII encryption in every table | Implemented, under a new key (B-13); History objects inherit their parent's PII, policy changes reach every version and are purged from history (B-21, B-24), keys rotate (B-27) | `encrypt.py`, `scd2.apply_pii_policy`, `scd2.rotate_key`, `tasks.purge`, `seed.legacy_to_plaintext` | `test_encrypt.py`, `test_scd2.py`, `test_seed.py`, `test_tasks.py`, `dq.check_pii` |
| FR-11 | Authorised users and processes can decrypt | Implemented; `secret()` in a UC function unconfirmed (Q4); the PII key in its own scope (B-22) | `ops.ensure_uc_objects` (`decrypt_pii`, grants) | UAT |
| FR-12 | Views for BI and data science | Implemented | `views.refresh_views` | `test_deletes_views_dq.py` |
| FR-13 | Datetimes +7 h Bangkok, including history DateTime values | Implemented | `transform.py` | `test_transform.py`, `test_scd2.py` |

## Non-functional

| ID | Requirement | Status | Implemented in |
| --- | --- | --- | --- |
| NFR-01 | All Salesforce traffic through the NAT gateway | Platform; the bundle uses classic compute only | `databricks.yml` (`sf_cluster`) |
| NFR-02 | Azure Databricks with Unity Catalog, classic compute | Implemented | `databricks.yml` (DBR 18 LTS Photon, single node) |
| NFR-03 | Tens of millions of rows, hundreds of fields | Implemented; single-node sizing to confirm in UAT (Q6) | Paged Bulk extraction to files, distributed CSV parse, `split_full_reload`, parallel objects |
| NFR-04 | Start at 00:30 Asia/Bangkok | Implemented | `resources/sf_nightly.job.yml` |
| NFR-05 | Re-running a failed night: same result, no duplicates or gaps | Implemented | Idempotent MERGE, watermark after commit, repair skips finished objects, Bulk count check (`test_scd2.py`, `test_tasks.py`, `test_bulk.py`) |
| NFR-06 | One workspace, one catalog per entity, country and environment | Implemented | `config.load_entity_env` (catalog name), one job per catalog |
| NFR-07 | Deploy with Databricks Asset Bundles through CI/CD | Partly: bundle ready, deploys into the service principal's folder (B-29); the CI pipeline itself is not in this repository | `databricks.yml`, `resources/` |
| NFR-08 | Row counts, watermarks and failures per object; failures alert | Implemented; per-object freshness (B-23) | `ops_salesforce.run_events` / `run_log`, job failure email, SQL alerts, dashboard, `dq.check_freshness` |

## Design decisions

All of D-01 to D-25 are implemented as designed, except where [decision-log.md](decision-log.md) says otherwise:

| Design decision | Departure |
| --- | --- |
| D-04 job tasks (for-each extract and load, separate deletes task) | One ingest task (B-01) |
| D-07 reuse the Synapse key and IV | A new random key in Spark's native format; the old one only decrypts the backup (B-13) |
| Design: Initial load from Synapse | Manual deep clone into `backup_salesforce`, then the seed job (B-14) |
| Design: Policy views show `_is_deleted` / `_is_archived` | Business columns only (B-15) |
| D-04 cluster (autoscaling) | Single node with Photon (B-16) |
| Design: weekly maintenance job | Predictive optimization (B-17), plus the purge task on tables whose PII was rewritten (B-21) |
| D-17 no record filters in SOQL | One exception for a corrupted Contract record (B-08) |
| D-18 hard deletes | A record that reappears becomes current again (B-04); incremental objects are checked weekly too (B-25) |
| D-20 PII config | History objects inherit their parent's PII (B-21); `decrypt_fields` (B-24); validation against Salesforce and the pipeline (B-26) |
| Design: one secret scope per environment, READ for `pii_readers` | Separate Salesforce and per-entity PII scopes (B-22) |
| Design: approvals in `field_approval` | Approvals in `config/approvals/<entity>.yml` (B-28) |
| B-19: a missing object is skipped | Failed in PROD unless paused (B-23) |
