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

- [ ] First run: note the ingest duration and the slowest objects in the per-object table, and the node's CPU and memory in the cluster metrics. Adjust `workers`, or choose a bigger node, from them (open question Q6). Check the durations leave room under the task timeouts (`long_task_timeout_seconds`, 6 hours, for ingest and seed; 1 hour for the other tasks), and raise them if a first full load needs more (B-20).
- [ ] Second run: the "longest first" list at the start of the ingest output starts with the slowest objects of the first run.

## 6. Hard deletes and backfill

- [ ] Deactivating a UAT user changes nothing in the hard-delete check: users are never deleted, only deactivated.
- [ ] Removing a member from a public group flags its GroupMember record deleted the next night and hides it in the view.
- [ ] Approve a pending field, let one nightly run pass, then run `sf_backfill_roojai` with that field. Current versions get the value, closed versions stay null and the watermark doesn't move.
