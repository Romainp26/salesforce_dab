# Open questions

What is unresolved, what it blocks and who can answer it. Remove a question once it is answered, and record the outcome in [decision-log.md](decision-log.md). Never reuse a number.

| ID | Question | Blocks | Owner |
| --- | --- | --- | --- |
| Q2 | **Is the corrupted Contract record the cause of Contract's `INVALID_DATE_FORMAT` in UAT?** Possibly. If a UAT run with `objects = Contract` and the new `soql_filter` succeeds, unpause Contract in UAT. Payments__c fails the same way and is still unexplained: find its records with a REST query. | Unpausing in UAT | Data engineering, Salesforce admins |
| Q3 | Does Bulk API 2.0 `queryAll` work on the feed objects (AccountFeed, Product2Feed, TaskFeed) and the Process objects? The REST fallback covers a rejection at job creation; a failure later in the job would still fail the object. (Design open question.) | — | First UAT run |
| Q4 | Does `secret()` work inside the `decrypt_pii` UC function and in column masks, with the permission model the design expects? (Design open question.) | FR-11 in PROD | Platform team |
| Q5 | Values still `TODO`: workspace host, service principal IDs, cluster policy IDs, SQL warehouse ID, alert and data-owner emails, secret scopes, org domains and integration usernames. salesforce_dab's working values can help: its workspace and `TH-BI-databricks-kv` scope. | First deploy | Platform team |
| Q6 | **Single node sizing.** The cluster is one `Standard_D4ds_v5` (4 vCPU, 16 GB) with Photon (B-16). salesforce_dab ran on a 32 GB `Standard_L4aos_v4` driver plus one worker. Check memory on the largest objects' first full load (UAT checklist §5); if it is short, a memory-optimized node such as `Standard_E4ds_v5` (4 vCPU, 32 GB) keeps the same cores. Also confirm `18.x-photon-scala2.13` is the workspace's runtime key. | PROD sizing | Data engineering |
| Q7 | PII key rotation has no tooling. The runbook (§5.12) describes the steps, but the re-encryption script doesn't exist. | Key rotation | Data engineering |
| Q9 | Kumka's object and field lists, `encrypted_fields` and archival users (design: Rollout). | Phase 2 | Kumka business owners |
| Q10 | CI/CD pipeline: the bundle is ready, but the pipeline (tests, `bundle validate`, deploy on merge and on release tag) isn't in this repository. | NFR-07 | Platform team |
