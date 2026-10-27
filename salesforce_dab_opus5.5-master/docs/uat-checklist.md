# UAT checklist

Run these in `roojai_th_uat` before the first PROD deploy. Most cases come from running salesforce_dab against the same Roojai UAT org; the unit tests named below cover the code side of each.

Look at the ingest task's output and `ops_salesforce.run_log` for each run:

```sql
SELECT object, status, last_stage, rows_extracted, sf_count, versions_written, error_message
FROM roojai_th_uat.ops_salesforce.run_log
WHERE run_id = '<job run id>'
ORDER BY status, object
```

## 1. Seed and encryption

- [ ] Clone a few objects into `roojai_th_uat.backup_salesforce` (Account, AccountHistory, Contact, a large one) and run `sf-seed-roojai_th_uat` with `objects` set to them ([runbook.md](runbook.md) §5.13). Expected:
  - every object succeeds; `seed_count` passes
  - the output lists the fields the backup lacks
  - a failure "don't decrypt with the legacy key" means `pii.legacy` is wrong (scope, secret names or encodings).
- [ ] As a member of `pii_readers_roojai_th_uat`, decrypt a few seeded values with `ops_salesforce.decrypt_pii` and compare them with Salesforce, for a person Account (`Name`, `Active_Email__c`), an AccountHistory `PersonEmail` change and a long address (over 40 characters).
- [ ] List the `Field` values each History table of an object with PII holds: `SELECT Field, count(*) FROM roojai_th_uat.bronze_salesforce_scd2.accounthistory GROUP BY Field`. Every value that records a PII field must be that field's name or an alias in `encrypt.HISTORY_FIELD_ALIASES` (AccountHistory writes `TextName` for `Name`); add any other alias found, and check `pii_is_ciphertext` passed for the History table.
- [ ] After the first nightly run, a record changed in Salesforce since the backup has a second version whose PII decrypts the same way, and `pii_is_ciphertext` passes.
- [ ] Check that no PII column of the seeded tables still holds legacy ciphertext: `pii_is_ciphertext` passes, and a sample of `decrypt_pii` values is readable text.

Unit tests:
- `test_encrypt.py::test_legacy_ciphertext_decrypts_in_every_stored_form`
- `test_seed.py::test_backup_rows_become_first_versions_re_encrypted`
- `test_seed.py::test_ciphertext_the_legacy_key_cant_read_fails_with_counts`
- `test_spark_output_is_byte_identical_to_reference`

## 2. Contract and Payments__c (corrupted records, INVALID_DATE_FORMAT)

Contract record `8002r000000vRMSAA2` is corrupted: any query returning it fails Salesforce-side. Contract's `soql_filter` (`Id != '8002r000000vRMSAA2'`) keeps it out of every query (B-08). In the UAT org, salesforce_dab saw Contract and Payments__c fail with `INVALID_DATE_FORMAT: Invalid DateTime`, so both are paused in UAT (`paused: [uat]`). PROD runs them.

- [ ] Nightly run: both show `status = 'skipped'`, `error_message = 'paused in uat'`, and the run succeeds.
- [ ] Run the job with `objects = Contract`. If it succeeds, the corrupted record was the cause: remove `paused` from Contract (open question Q2).
- [ ] Run the job with `objects = Payments__c`. If it fails, expected:
  - it fails at `last_stage = 'extract'` with the Salesforce error in `error_message`
  - no SCD2 table or watermark is created for it
  - its landing folder is gone
  - the job fails and the on-failure email arrives.
- [ ] For an object that still fails, find the offending records with a REST query on the UAT org and decide with the Salesforce admins: fix the data, or add a `soql_filter` for records that can't be returned at all.
- [ ] In PROD, the first nightly run must ingest both, and `count_matches_soql` must pass for Contract: the filter applies to its `COUNT()` too.

Unit tests:
- `test_extract.py::test_soql_filter_applies_to_every_query_shape`
- `test_tasks.py::test_a_failed_object_never_loads_and_never_stops_the_others`
- `test_paused_objects_are_skipped_unless_requested`
- `test_extract.py::test_other_bulk_errors_are_not_retried_through_rest`

## 3. Objects Bulk API 2.0 rejects

ContractStatus and TaskStatus are configured with `api: rest`. Whether queryAll works on the feed objects (AccountFeed, Product2Feed, TaskFeed) and the Process objects is still open (design: Open questions).

- [ ] After the first nightly run, search the ingest output for `the Bulk API doesn't support this object`. Each object it names fell back to REST queryAll and should get `api: rest` in `config/objects/roojai.yml`.
- [ ] Every configured object shows `succeeded` or, for the two paused ones, `skipped`.
- [ ] An object that exists in PROD but not in the UAT org shows `status = 'skipped'`, `last_stage = 'preflight'`, `error_message = 'not in the uat org'`, with an `object_planned` warning, and the other objects still run (B-19). Decide with the Salesforce admins whether to create it in UAT or pause it there.

Unit tests:
- `test_extract.py::test_objects_the_bulk_api_rejects_fall_back_to_rest`
- `test_tasks.py::test_one_object_that_cant_be_planned_never_stops_the_others`

## 4. Repair and re-run

- [ ] Cancel a nightly run during the ingest task, then repair it. Objects that finished are skipped ("already ingested in this run"). The others finish, with no duplicate versions: the `one_current_row_per_id` check passes.
- [ ] Run the nightly job twice in a row. The second run writes almost no versions: only the 1-hour overlap is re-read, and replays are no-ops.

## 5. Timing and ordering

- [ ] First run: note the ingest duration and the slowest objects in the per-object table, and the node's CPU and memory in the cluster metrics. Adjust `workers`, or choose a bigger node, from them (open question Q6). Check the durations leave room under the task timeouts (`long_task_timeout_seconds`, 6 hours, for ingest, backfill, seed and rotate; 1 hour for the other tasks), and raise them if a first full load needs more (B-20).
- [ ] Second run: the "longest first" list at the start of the ingest output starts with the slowest objects of the first run.

## 6. Hard deletes and backfill

- [ ] Deactivating a UAT user changes nothing in the hard-delete check: users are never deleted, only deactivated.
- [ ] Removing a member from a public group flags its GroupMember record deleted the next night and hides it in the view.
- [ ] Approve a pending field, let one nightly run pass, then run `sf_backfill_roojai` with that field. Current versions get the value, closed versions stay null and the watermark doesn't move.

## 7. PII policy changes and purges (B-21, B-24)

Use a UAT-only test field, e.g. a text `Audit_Email__c` and a date `Audit_Date__c` on a small object with field history tracking on, so production objects stay untouched.

- [ ] **Encrypt an existing text field.** Add it to `encrypted_fields`, deploy, run the nightly job (no record needs to change). Expected: every version holds ciphertext; `DESCRIBE HISTORY` shows an `UPDATE` tagged `sf_ingest_pii:<table>:encrypt:<field>:1`; the History table's `OldValue`/`NewValue` for that field are ciphertext; `pii_is_ciphertext` passed over every row of the object.
- [ ] **The old plaintext is gone.** Note the version before the UPDATE (`pii_purge.version`). After the run, `pii_purge` shows `reorged`, with the data files still to delete in `files`, and `SHOW TBLPROPERTIES` lists `delta.deletedFileRetentionDuration = interval 12 hours`. After the next night's purge (12 hours later at least): `pii_purge` shows `files_purged`, `pii_history_purged` passed, `VACUUM <table> DRY RUN` lists none of the recorded files, and `SELECT * FROM <table> VERSION AS OF <version>` fails. Two nights after that, `pii_purge` shows `done`, `DESCRIBE HISTORY` starts after the purge, and `SHOW TBLPROPERTIES` no longer lists `delta.deletedFileRetentionDuration`.
- [ ] **Encrypt a date field.** As above for `Audit_Date__c`: the column becomes a STRING, and `decrypt_pii` gives back `yyyy-MM-dd`. A record changed after that night has the same ciphertext for the same date (B-24). Also confirm the grants on the table survived the rewrite (Q15).
- [ ] **Decrypt it again.** Remove the field from `encrypted_fields` without `decrypt_fields`: the object fails with "no longer in the PII config". Add `decrypt_fields: [Audit_Date__c]`: every version is a DATE again, plaintext.
- [ ] **A typo.** Misspell a field in `encrypted_fields`: preflight fails the object with "are not in Salesforce's describe".

Unit tests:
- `test_scd2.py::test_newly_flagged_pii_is_encrypted_once_in_every_version`
- `test_scd2.py::test_encrypting_a_typed_column_rewrites_it_as_string_ciphertext`
- `test_scd2.py::test_a_field_leaving_the_pii_config_is_decrypted_only_when_acknowledged`
- `test_tasks.py::test_purge_removes_the_plaintext_history_of_a_re_encrypted_table`
- `test_tasks.py::test_the_purge_waits_for_the_file_retention_before_vacuuming`
- `test_metadata.py::test_pii_fields_must_be_described`

## 8. Key rotation (B-27)

- [ ] Follow runbook §5.12 in UAT with a second key: the nightly job refuses PII objects between the deploy and the rotation; after `sf-rotate-roojai_th_uat`, `decrypt_pii` reads every value, `_pii_key_version` is 2 on every row of every object with encrypted values (and their `*History` objects; tables without PII keep the version they were written with), and the purge runs.

Unit test: `test_scd2.py::test_key_rotation_re_encrypts_and_guards_the_nightly_load`

## 9. Deletes, schema drift and safeguards

- [ ] **Hard delete on an incremental object (B-25).** Hard-delete a test record (Data Loader "hard delete") and run the nightly job on its object's check night (or with `reconcile.every_days: 1`). Expected: a `reconcile` event with `hard_deletes = 1`, the record's current version `_is_hard_deleted`, gone from the view.
- [ ] **Archival (D-23, Q13).** Delete a test record as an archival user: it arrives `_is_archived`. If it doesn't arrive at all, the archival users hard-delete: raise Q13.
- [ ] **Decimal places (B-32).** Raise a test number field's decimal places: the next run widens the column, no value is rounded, `field_retyped` reports the precision.
- [ ] **Missing object in PROD (B-23).** Not testable in UAT: confirm with the unit test, and in PROD watch the first nights' `object_planned` warnings.
- [ ] **Seed guard (B-34).** In a fresh catalog with a backup table, run the nightly job before the seed: the object fails with "hasn't been seeded".
- [ ] **Approvals (B-28).** Approve a pending field by pull request in `config/approvals/roojai.yml`: after deploying to UAT and PROD, both environments query it.

Unit tests:
- `test_tasks.py::test_incremental_objects_are_checked_for_hard_deletes_when_due`
- `test_deletes_views_dq.py::test_hard_delete_check_of_an_incremental_object`
- `test_scd2.py::test_numeric_type_changes_widen_or_fail_but_never_round`
- `test_tasks.py::test_a_missing_object_fails_in_prod`
- `test_tasks.py::test_a_first_load_never_runs_before_the_seed`
- `test_tasks.py::test_preflight_applies_new_field_policies_and_approvals`
