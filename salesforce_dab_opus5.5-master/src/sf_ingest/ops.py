"""ops_salesforce: setup DDL, control tables, run events and watermarks (D-15, NFR-08).

The ingest task's worker threads only append to run_events and watermark_history; the run_log and watermarks
views aggregate them, so concurrent objects never conflict on a MERGE.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import OPS_SCHEMA, SCD2_SCHEMA, VIEW_SCHEMA, AccessGroups
from .encrypt import CipherSpec, decrypt_at_sql

log = logging.getLogger(__name__)


def ops_table(catalog: str, name: str) -> str:
    return f"`{catalog}`.`{OPS_SCHEMA}`.`{name}`"


_TABLES = {
    "object_config": "object STRING, config_json STRING, loaded_at TIMESTAMP",
    "field_allowlist": "object STRING, field STRING, allowed INT, source STRING, updated_at TIMESTAMP",
    "field_approval": (
        "object STRING, field STRING, sf_type STRING, status STRING, detected_at TIMESTAMP, "
        "decided_by STRING, decided_at TIMESTAMP, applied_at TIMESTAMP"
    ),
    "object_schema": (
        "object STRING, field STRING, sf_type STRING, length INT, precision INT, scale INT, "
        "run_id STRING, captured_at TIMESTAMP"
    ),
    "run_plan": "run_id STRING, object STRING, fields_json STRING, created_at TIMESTAMP",
    "run_events": (
        "run_id STRING, object STRING, stage STRING, status STRING, event_at TIMESTAMP, bulk_job_id STRING, "
        "files_landed BIGINT, rows_extracted BIGINT, sf_count BIGINT, versions_written BIGINT, "
        "soft_deletes BIGINT, archivals BIGINT, hard_deletes BIGINT, watermark_before TIMESTAMP_NTZ, "
        "watermark_after TIMESTAMP_NTZ, error_message STRING"
    ),
    "watermark_history": "object STRING, watermark_utc TIMESTAMP_NTZ, run_id STRING, recorded_at TIMESTAMP",
    "certificate": "checked_at TIMESTAMP, expires_at TIMESTAMP, subject STRING, run_id STRING",
    "dq_results": (
        "run_id STRING, check_name STRING, object STRING, severity STRING, passed BOOLEAN, "
        "detail STRING, checked_at TIMESTAMP"
    ),
    # Append-only: the latest row per key says whether it is `encrypted` (also a null action, older rows) or
    # `decrypted` (B-24).
    "encryption_state": (
        "object STRING, pii_key STRING, key_version INT, recorded_at TIMESTAMP, action STRING, run_id STRING"
    ),
    "view_state": "object STRING, definition_hash STRING, updated_at TIMESTAMP",
    # Append-only purge requests of SCD2 history left behind by in-place PII rewrites (B-21): `requested`, then
    # `reorged` (REORG done; `files`: JSON list of the data files to delete), `files_purged` (VACUUM done, `version`
    # is the table version then), and `done` once the log is cleaned.
    "pii_purge": (
        "object STRING, action STRING, reason STRING, version BIGINT, run_id STRING, event_at TIMESTAMP, "
        "started_at TIMESTAMP, files STRING"
    ),
}

_RUN_LOG_VIEW = """
CREATE OR REPLACE VIEW {run_log} AS
SELECT
  run_id,
  object,
  min(event_at) AS started_at,
  max(event_at) AS ended_at,
  max(bulk_job_id) AS bulk_job_id,
  max(files_landed) AS files_landed,
  max(rows_extracted) AS rows_extracted,
  max(sf_count) AS sf_count,
  max(versions_written) AS versions_written,
  max(soft_deletes) AS soft_deletes,
  max(archivals) AS archivals,
  max(hard_deletes) AS hard_deletes,
  max(watermark_before) AS watermark_before,
  max(watermark_after) AS watermark_after,
  max_by(stage, event_at) AS last_stage,
  -- The latest event decides, so an object that failed and then succeeded in a repair of the same run reads
  -- succeeded; error_message keeps the last error either way.
  max_by(status, event_at) AS status,
  max_by(error_message, CASE WHEN error_message IS NOT NULL THEN event_at END) AS error_message
FROM {run_events}
GROUP BY run_id, object
"""

_WATERMARKS_VIEW = """
CREATE OR REPLACE VIEW {watermarks} AS
SELECT object, max(watermark_utc) AS watermark_utc, max(recorded_at) AS recorded_at
FROM {history}
GROUP BY object
"""


def ensure_ops_tables(spark: SparkSession, catalog: str) -> None:
    for schema in (SCD2_SCHEMA, VIEW_SCHEMA, OPS_SCHEMA):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    for name, columns in _TABLES.items():
        fqn = ops_table(catalog, name)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {fqn} ({columns}) USING DELTA")
        # Columns added by a later version of the bundle reach tables created by an earlier one.
        existing = {f.lower() for f in spark.table(fqn).schema.fieldNames()}
        missing = [c for c in columns.split(", ") if c.split(" ")[0].lower() not in existing]
        if missing:
            spark.sql(f"ALTER TABLE {fqn} ADD COLUMNS ({', '.join(missing)})")
    spark.sql(_RUN_LOG_VIEW.format(run_log=ops_table(catalog, "run_log"), run_events=ops_table(catalog, "run_events")))
    spark.sql(
        _WATERMARKS_VIEW.format(
            watermarks=ops_table(catalog, "watermarks"), history=ops_table(catalog, "watermark_history")
        )
    )


def ensure_uc_objects(
    spark: SparkSession,
    catalog: str,
    spec: CipherSpec,
    access: AccessGroups,
    previous: dict[int, CipherSpec] | None = None,
) -> list[str]:
    """Unity Catalog-only setup: predictive optimization, landing volume, decrypt functions and grants (B-29).

    Returns warnings.
    """
    warnings = []
    # Predictive optimization runs OPTIMIZE, VACUUM and ANALYZE on the managed tables: no maintenance job.
    for schema in (SCD2_SCHEMA, OPS_SCHEMA):
        try:
            spark.sql(f"ALTER SCHEMA `{catalog}`.`{schema}` ENABLE PREDICTIVE OPTIMIZATION")
        except Exception as exc:
            warnings.append(f"Enabling predictive optimization on {schema} failed: {type(exc).__name__}")
    # Created by the service principal and never granted: of the groups, only it can read plaintext. Catalog owners
    # and metastore admins still can (architecture: PII protection).
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{OPS_SCHEMA}`.`landing`")
    fn = f"`{catalog}`.`{OPS_SCHEMA}`.`decrypt_pii`"
    spark.sql(f"CREATE OR REPLACE FUNCTION {fn}(value STRING) RETURNS STRING RETURN {spec.decrypt_sql('value')}")
    # decrypt_pii_at(col, _pii_key_version) reads a row with the key it was written with, mid-rotation too (B-27).
    at = decrypt_at_sql("value", "key_version", spec, previous or {})
    spark.sql(
        f"CREATE OR REPLACE FUNCTION `{catalog}`.`{OPS_SCHEMA}`.`decrypt_pii_at`(value STRING, key_version INT) "
        f"RETURNS STRING RETURN {at}"
    )
    for statement, group in grant_statements(catalog, access):
        try:
            spark.sql(f"{statement} TO `{group}`")
        except Exception as exc:
            warnings.append(f"{statement} TO {group} failed: {type(exc).__name__}")
    return warnings


def grant_statements(catalog: str, access: AccessGroups) -> list[tuple[str, str]]:
    """(GRANT ..., group) for every group of runbook §6. The landing volume is never granted: plaintext."""
    cat = f"`{catalog}`"

    def schema(name: str) -> str:
        return f"`{catalog}`.`{name}`"

    out = []
    for group, schema_name, privilege in (
        (access.readers, VIEW_SCHEMA, "SELECT"),
        (access.history_readers, SCD2_SCHEMA, "SELECT"),
        (access.operators, OPS_SCHEMA, "SELECT"),
        (access.pii_readers, OPS_SCHEMA, None),
    ):
        out += [
            (f"GRANT USE CATALOG ON CATALOG {cat}", group),
            (f"GRANT USE SCHEMA ON SCHEMA {schema(schema_name)}", group),
        ]
        if privilege:
            out.append((f"GRANT {privilege} ON SCHEMA {schema(schema_name)}", group))
    for fn in ("decrypt_pii", "decrypt_pii_at"):
        out.append((f"GRANT EXECUTE ON FUNCTION `{catalog}`.`{OPS_SCHEMA}`.`{fn}`", access.pii_readers))
    return out


def now_utc() -> datetime:
    return datetime.now(UTC)


def append_rows(spark: SparkSession, catalog: str, table: str, rows: list[dict]) -> None:
    """Append dicts to an ops table; keys missing from a dict are written as null."""
    if not rows:
        return
    fqn = ops_table(catalog, table)
    schema = spark.table(fqn).schema
    data = [tuple(r.get(name) for name in schema.fieldNames()) for r in rows]
    spark.createDataFrame(data, schema).write.format("delta").mode("append").saveAsTable(fqn)


def append_event(spark: SparkSession, catalog: str, run_id: str, obj: str, stage: str, status: str, **metrics):
    if metrics.get("error_message"):
        metrics["error_message"] = str(metrics["error_message"])[:2000]
    row = dict(run_id=run_id, object=obj, stage=stage, status=status, event_at=now_utc(), **metrics)
    append_rows(spark, catalog, "run_events", [row])


def append_dq(spark: SparkSession, catalog: str, run_id: str, results: list[dict]) -> None:
    """results: dicts with check_name, object, severity ('critical'|'warning'), passed, detail."""
    at = now_utc()
    append_rows(spark, catalog, "dq_results", [{**r, "run_id": run_id, "checked_at": at} for r in results])


def sql_string(value: str) -> str:
    """A quoted SQL string literal.

    The code avoids named SQL parameters: with Spark 4.1 and Delta 4.3 they come back unbound in Delta statements
    and in queries over Delta tables and views.
    """
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def get_watermark(spark: SparkSession, catalog: str, obj: str) -> datetime | None:
    rows = spark.table(ops_table(catalog, "watermarks")).where(F.col("object") == obj).collect()
    if not rows or rows[0].watermark_utc is None:
        return None
    return rows[0].watermark_utc.replace(tzinfo=UTC)


def record_watermark(spark: SparkSession, catalog: str, obj: str, watermark_utc: datetime, run_id: str) -> None:
    """Append-only; the watermarks view takes the max, so a full reload never moves it backwards."""
    row = {
        "object": obj,
        "watermark_utc": watermark_utc.astimezone(UTC).replace(tzinfo=None),
        "run_id": run_id,
        "recorded_at": now_utc(),
    }
    append_rows(spark, catalog, "watermark_history", [row])


def write_run_plan(spark: SparkSession, catalog: str, run_id: str, plans: dict[str, list[dict]]) -> None:
    spark.sql(f"DELETE FROM {ops_table(catalog, 'run_plan')} WHERE run_id = {sql_string(run_id)}")
    at = now_utc()
    rows = [dict(run_id=run_id, object=o, fields_json=json.dumps(f), created_at=at) for o, f in plans.items()]
    append_rows(spark, catalog, "run_plan", rows)


def read_run_plan(spark: SparkSession, catalog: str, run_id: str, obj: str) -> list[dict]:
    rows = (
        spark.table(ops_table(catalog, "run_plan"))
        .where((F.col("run_id") == run_id) & (F.col("object") == obj))
        .select("fields_json")
        .collect()
    )
    if not rows:
        raise RuntimeError(
            f"No run plan for {obj} in run {run_id}: preflight skipped or failed it (see run_log), or didn't run"
        )
    return json.loads(rows[0].fields_json)


def planned_objects(spark: SparkSession, catalog: str, run_id: str) -> set[str]:
    rows = spark.table(ops_table(catalog, "run_plan")).where(F.col("run_id") == run_id).select("object").collect()
    return {r.object for r in rows}


def preflight_outcomes(spark: SparkSession, catalog: str, run_id: str) -> dict[str, tuple[str, str]]:
    """(status, error) of the objects preflight skipped or failed in this run, from their latest preflight event."""
    rows = (
        spark.table(ops_table(catalog, "run_events"))
        .where((F.col("run_id") == run_id) & (F.col("stage") == "preflight"))
        .groupBy("object")
        .agg(F.max_by("status", "event_at").alias("status"), F.max_by("error_message", "event_at").alias("error"))
        .collect()
    )
    return {r.object: (r.status, r.error or "") for r in rows}


def encryption_events(spark: SparkSession, catalog: str, obj: str) -> dict[str, tuple[str, int]]:
    """{pii key: (latest action, number of state rows)}; the count numbers the next migration's commit tag."""
    rows = spark.table(ops_table(catalog, "encryption_state")).where(F.col("object") == obj).collect()
    out: dict[str, tuple[str, int]] = {}
    for r in sorted(rows, key=lambda r: r.recorded_at):
        _, n = out.get(r.pii_key, ("", 0))
        out[r.pii_key] = (r.action or "encrypted", n + 1)
    return out


def encryption_state(spark: SparkSession, catalog: str, obj: str) -> set[str]:
    """PII keys whose existing values are encrypted."""
    return {k for k, (action, _) in encryption_events(spark, catalog, obj).items() if action == "encrypted"}


def record_encryption(
    spark: SparkSession,
    catalog: str,
    obj: str,
    keys: list[str],
    key_version: int,
    action: str = "encrypted",
    run_id: str | None = None,
) -> None:
    at = now_utc()
    rows = [
        dict(object=obj, pii_key=k, key_version=key_version, recorded_at=at, action=action, run_id=run_id) for k in keys
    ]
    append_rows(spark, catalog, "encryption_state", rows)


def request_purge(
    spark: SparkSession,
    catalog: str,
    obj: str,
    reason: str,
    version: int,
    run_id: str,
    files: list[str] | None = None,
) -> None:
    """Ask the purge task to remove the history an in-place PII rewrite left behind (B-21). `files`: the table's data
    files before the rewrite, so the purge knows them without time travel, which may be refused by then."""
    row = dict(
        object=obj,
        action="requested",
        reason=reason,
        version=version,
        run_id=run_id,
        event_at=now_utc(),
        files=json.dumps(files) if files is not None else None,
    )
    append_rows(spark, catalog, "pii_purge", [row])


def record_purge(
    spark: SparkSession,
    catalog: str,
    obj: str,
    action: str,
    version: int | None,
    run_id: str,
    started_at: datetime | None = None,
    files: list[str] | None = None,
) -> None:
    """`started_at`: when the purge read the state it acted on, so a request or step another job recorded since
    stays pending."""
    row = dict(
        object=obj,
        action=action,
        reason=None,
        version=version,
        run_id=run_id,
        event_at=now_utc(),
        started_at=started_at,
        files=json.dumps(files) if files is not None else None,
    )
    append_rows(spark, catalog, "pii_purge", [row])


def pending_purges(spark: SparkSession, catalog: str) -> dict[str, dict]:
    """{object: {"state": "requested" | "reorged" | "files_purged", "version", "reasons", "files", "at"}} for
    unfinished purges (B-21). `at` is the time of the latest step; `files` the data files still to delete."""
    out: dict[str, dict] = {}
    rows = spark.table(ops_table(catalog, "pii_purge")).collect()
    for r in sorted(rows, key=lambda r: r.event_at):
        cur = out.get(r.object)
        # A step decided on a state read before another job's request or REORG doesn't cover it.
        stale = r.started_at is not None and cur is not None and cur.get("at") is not None and cur["at"] > r.started_at
        if r.action == "done":
            if not stale:
                out.pop(r.object, None)
        elif r.action == "requested":
            if cur is None:
                cur = out[r.object] = {"state": "requested", "version": r.version, "reasons": [], "files": []}
            if r.files:
                cur["files"] = sorted(set(cur["files"]) | set(json.loads(r.files)))
            elif cur["state"] != "requested":
                # A rewrite after the REORG needs another one, from its own version; files recorded so far stay.
                cur.update(state="requested", version=r.version)
            elif r.version is not None and (cur["version"] is None or r.version < cur["version"]):
                cur["version"] = r.version
            if r.reason not in cur["reasons"]:
                cur["reasons"].append(r.reason)
            cur["requested_at"] = cur["at"] = r.event_at
        elif r.action == "reorged" and cur is not None:
            # A request made while the REORG ran (a backfill alongside) isn't covered by it: stays requested.
            if r.started_at is None or cur.get("requested_at") is None or cur["requested_at"] <= r.started_at:
                cur.update(state="reorged", version=r.version, files=json.loads(r.files or "[]"), at=r.event_at)
            else:
                cur["files"] = sorted(set(cur["files"]) | set(json.loads(r.files or "[]")))
        elif r.action == "files_purged" and cur is not None and cur["state"] == "reorged" and not stale:
            cur.update(state="files_purged", version=r.version, files=[], at=r.event_at)
    return out


def ingested_objects(spark: SparkSession, catalog: str, run_id: str) -> set[str]:
    """Objects whose ingest finished in this run. A repair of the run skips them."""
    rows = (
        spark.table(ops_table(catalog, "run_events"))
        .where((F.col("run_id") == run_id) & (F.col("stage") == "ingest") & (F.col("status") == "succeeded"))
        .select("object")
        .distinct()
        .collect()
    )
    return {r.object for r in rows}


def last_succeeded(spark: SparkSession, catalog: str, stage: str) -> dict[str, datetime]:
    """Latest time each object's `stage` event succeeded (e.g. "ingest", "reconcile"), in UTC."""
    rows = (
        spark.table(ops_table(catalog, "run_events"))
        .where((F.col("stage") == stage) & (F.col("status") == "succeeded"))
        .groupBy("object")
        .agg(F.max("event_at").alias("at"))
        .collect()
    )
    return {r.object: r.at.replace(tzinfo=UTC) if r.at.tzinfo is None else r.at for r in rows}


def recent_durations(spark: SparkSession, catalog: str, days: int = 14) -> dict[str, float]:
    """Average seconds of each object's successful nightly ingests over the last `days` days."""
    rows = spark.sql(
        f"SELECT object, avg(timestampdiff(SECOND, started_at, ended_at)) AS seconds "
        f"FROM {ops_table(catalog, 'run_log')} "
        "WHERE status = 'succeeded' AND last_stage = 'ingest' "
        f"AND started_at >= current_timestamp() - INTERVAL {int(days)} DAYS "
        "GROUP BY object"
    ).collect()
    return {r.object: float(r.seconds) for r in rows if r.seconds is not None}
