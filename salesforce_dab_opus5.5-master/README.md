# Salesforce ingestion (Databricks Asset Bundle)

Every night at 00:30 Bangkok time, this bundle copies Salesforce Sales Cloud data (Roojai now, Kumka in phase 2) into Unity Catalog on Azure Databricks. For each object it:

1. extracts the changed records with Bulk API 2.0 `queryAll`
2. lands them as raw CSV pages in a volume
3. merges them into SCD2 Delta tables that keep every version, encrypting PII on the way
4. publishes views that apply each object's delete, created-today and filter policies.

BI and data science query the views.

The agreed requirements and design are in `shared_folder/salesforce-ingestion-design.md` (IDs FR-xx, NFR-xx, D-xx). The bundle's own decisions are in [docs/decision-log.md](docs/decision-log.md) (IDs B-xx).

## Documentation

| Document | Read it to |
| --- | --- |
| [docs/architecture.md](docs/architecture.md) | Understand how it works: jobs and tasks, extraction, SCD2, timestamps, deletes, views, encryption, ops tables |
| [docs/configuration.md](docs/configuration.md) | Look up a setting: bundle variables, `entities.yml`, per-object keys, job parameters, constants |
| [docs/runbook.md](docs/runbook.md) | Operate it: daily checks, running jobs, reading a run, what to do when something fails, procedures |
| [docs/uat-checklist.md](docs/uat-checklist.md) | Validate it in UAT before PROD |
| [docs/requirements.md](docs/requirements.md) | Trace each design requirement to its implementation and tests |
| [docs/decision-log.md](docs/decision-log.md) | See why it departs from the design, and the alternatives set aside |
| [docs/open-questions.md](docs/open-questions.md) | See what is still open before PROD |

## At a glance

- **Jobs**, one set per catalog: nightly ingest, on-demand field backfill, one-off seed from `backup_salesforce` (the clone of the Synapse tables), on-demand PII key rotation. The nightly job is `preflight → ingest → views → dq → purge → cleanup` on one single-node classic cluster (Databricks Runtime 18 LTS, Photon) behind the NAT gateway. Predictive optimization maintains the tables. The ingest task runs objects in parallel, longest first, and its output tells the story of the night.
- **Catalogs**: `<entity>_th_<env>` (`roojai_th_uat`, `roojai_th_prod`), each with these schemas:
  - `bronze_salesforce_scd2`: SCD2 tables, every version
  - `bronze_salesforce`: policy views, current versions
  - `ops_salesforce`: run log, watermarks, field allowlist and approvals, DQ results, landing volume
  - `backup_salesforce`: the manual clone of the Synapse tables, read once by the seed.
- **Config**: `config/entities.yml` (orgs, secrets, PII key format, access groups), `config/objects/<entity>.yml` (124 Roojai objects and their policies) and `config/approvals/<entity>.yml` (decisions on new fields), reviewed by pull request and packaged in the wheel.
- **Safety**:
  - PII is encrypted deterministically (AES-CBC with a static IV) before it reaches a table, `*History` objects included, and decryptable only by `pii_readers_<catalog>`
  - in this pipeline, plaintext exists only in landing files, deleted after each object (outside it: `backup_salesforce` until it is dropped, and storage-level copies such as blob soft delete, Q14); a change to the PII config rewrites every version and then purges the old ones from the table's history (B-21)
  - the Salesforce JWT key and the PII key live in separate secret scopes (B-22)
  - runs are idempotent and repairable
  - failures and failed checks alert.

  The audit of 2026-10-26 and what changed after it: [docs/audit-2026-10-26.md](docs/audit-2026-10-26.md), decisions B-21 to B-38.

## Layout

| Path | What |
| --- | --- |
| `databricks.yml` | Bundle, `uat` / `prod` targets, variables, the job cluster |
| `resources/` | Jobs (nightly, backfill, seed, rotate), SQL alerts, dashboard |
| `config/` | `entities.yml`, `objects/<entity>.yml`, `approvals/<entity>.yml`, `seed/<entity>_fields_allowlist.csv` |
| `src/sf_ingest/` | The Python package (wheel). `tasks.py` is the entry point, one subcommand per job task |
| `dashboards/` | Ops dashboard |
| `scripts/generate_config.py` | One-off generator of the object config from the source CSVs |
| `shared_folder/` | Inputs: the design, the source allowlists and encrypted-field lists |
| `tests/` | Unit and local Spark + Delta tests |

## Quick start

```bash
uv sync && uv run pytest                     # Python 3.12; Spark tests need Java 17+
databricks bundle validate -t uat            # needs the Databricks CLI and workspace access
databricks bundle deploy -t uat
databricks bundle run -t uat sf_nightly_roojai
```

Before the first deploy, work through [docs/open-questions.md](docs/open-questions.md): the `TODO` values and the new PII key. Then go through [docs/uat-checklist.md](docs/uat-checklist.md).
