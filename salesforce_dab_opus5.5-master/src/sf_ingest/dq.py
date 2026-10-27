"""Data quality checks (design: Operations). Critical failures fail the run; warnings feed SQL alerts."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import ops
from .config import ObjectConfig, history_parent
from .encrypt import HISTORY_FIELD_ALIASES, has_history_values, is_ciphertext_sql, pii_predicates, quote
from .scd2 import scd2_table, table_exists

log = logging.getLogger(__name__)

CRITICAL = "critical"
WARNING = "warning"


def result(check: str, obj: str | None, severity: str, passed: bool, detail: str = "") -> dict:
    return {"check_name": check, "object": obj, "severity": severity, "passed": passed, "detail": detail}


def _written_by(run_id: str | None) -> str:
    """Rows written by this run, or every row (a full check) when run_id is None (B-37)."""
    return f"_run_id = {ops.sql_string(run_id)}" if run_id else "true"


def check_one_current(spark: SparkSession, table: str, obj: ObjectConfig, run_id: str | None = None) -> dict:
    touched = f"AND Id IN (SELECT Id FROM {table} WHERE {_written_by(run_id)})" if run_id else ""
    dupes = (
        spark.sql(
            f"SELECT count(*) AS n FROM (SELECT Id FROM {table} WHERE _is_current {touched} "
            "GROUP BY Id HAVING count(*) > 1)"
        )
        .first()
        .n
    )
    return result("one_current_row_per_id", obj.name, CRITICAL, dupes == 0, f"{dupes} Ids with several current rows")


def check_pii(spark: SparkSession, table: str, obj: ObjectConfig, run_id: str | None = None) -> list[dict]:
    columns = {c.lower() for c in spark.table(table).columns}
    out = []
    for col, applies in pii_predicates(obj, columns):
        bad = (
            spark.sql(
                f"SELECT count(*) AS n FROM {table} WHERE {_written_by(run_id)} "
                f"AND {quote(col)} IS NOT NULL AND ({applies}) AND NOT {is_ciphertext_sql(quote(col))}"
            )
            .first()
            .n
        )
        # Counts only: never log or store field values.
        out.append(result("pii_is_ciphertext", obj.name, CRITICAL, bad == 0, f"{col}: {bad} non-ciphertext values"))
    return out


# `Field` values of *History rows that record an event, not a field (Salesforce field history documentation).
HISTORY_EVENTS = {"created", "deleted", "locked", "unlocked", "feedevent", "owneraccepted", "ownerassignment"}


def check_history_fields(spark: SparkSession, catalog: str, table: str, obj: ObjectConfig) -> list[dict]:
    """Weekly: `Field` values of a History table with PII that match neither a field of its parent nor an alias
    (B-21). A PII field recorded under another name (AccountHistory's `TextName` for `Name`) would stay plaintext."""
    columns = {c.lower() for c in spark.table(table).columns}
    if not (has_history_values(columns) and pii_predicates(obj, columns)):
        return []
    parent = history_parent(obj.name)
    snapshot = spark.table(ops.ops_table(catalog, "object_schema")).where(F.lower("object") == parent.lower())
    known = {r.field.lower() for r in snapshot.select("field").collect()}
    if not known:
        return []
    known |= HISTORY_EVENTS | {a for aliases in HISTORY_FIELD_ALIASES.values() for a in aliases}
    values = {r.f for r in spark.sql(f"SELECT DISTINCT lower(Field) AS f FROM {table}").collect() if r.f}
    unknown = sorted(values - known)
    # Field names only, never values.
    detail = f"Field values matching no {parent} field: {unknown}" if unknown else ""
    return [result("history_field_unmapped", obj.name, WARNING, not unknown, detail)]


def check_run_log(spark: SparkSession, catalog: str, run_id: str, objects: dict[str, ObjectConfig]) -> list[dict]:
    rows = spark.table(ops.ops_table(catalog, "run_log")).where(f"run_id = {ops.sql_string(run_id)}").collect()
    out = []
    for r in rows:
        if r.sf_count is not None and r.rows_extracted is not None:
            out.append(
                result(
                    "count_matches_soql",
                    r.object,
                    WARNING,
                    r.sf_count == r.rows_extracted,
                    f"extracted {r.rows_extracted}, COUNT() {r.sf_count}",
                )
            )
        obj = objects.get(r.object)
        # Rows that only repeat the 1-hour overlap write no versions and need not advance the watermark.
        if obj and obj.load_type == "incremental" and r.versions_written:
            advanced = r.watermark_after is not None and (
                r.watermark_before is None or r.watermark_after > r.watermark_before
            )
            out.append(
                result(
                    "watermark_advanced",
                    r.object,
                    WARNING,
                    advanced,
                    f"before {r.watermark_before}, after {r.watermark_after}",
                )
            )
    return out


def check_pending_approvals(spark: SparkSession, catalog: str) -> list[dict]:
    rows = spark.sql(
        f"SELECT object, field FROM {ops.ops_table(catalog, 'field_approval')} WHERE status = 'pending'"
    ).collect()
    return [result("new_field_pending_approval", r.object, WARNING, False, r.field) for r in rows]


def check_freshness(
    spark: SparkSession,
    catalog: str,
    objects: tuple[ObjectConfig, ...],
    env: str,
    hours: int,
    now: datetime | None = None,
) -> list[dict]:
    """An object the nightly run should load but hasn't for `hours` (missing in the org, failing, B-23)."""
    now = now or datetime.now(UTC)
    last = ops.last_succeeded(spark, catalog, "ingest")
    out = []
    for obj in objects:
        if obj.retired or env in obj.paused:
            continue
        at = last.get(obj.name)
        age = (now - at).total_seconds() / 3600 if at else None
        detail = f"last successful ingest {age:.0f} hours ago" if at else "never ingested successfully"
        out.append(result("object_freshness", obj.name, WARNING, age is not None and age <= hours, detail))
    return out


def run_checks(
    spark: SparkSession,
    catalog: str,
    run_id: str,
    objects: tuple[ObjectConfig, ...],
    env: str = "uat",
    full: bool = False,
    freshness_hours: int = 36,
    rewritten: set[str] | None = None,
) -> list[dict]:
    """Every check of the dq task. Table checks cover the rows this run wrote, or every row with `full` (weekly) and
    for objects whose PII was rewritten in place tonight (`rewritten`, B-37)."""
    results = []
    rewritten = rewritten or set()
    for obj in objects:
        table = scd2_table(catalog, obj)
        if not table_exists(spark, table):
            continue
        scope = None if full or obj.name in rewritten else run_id
        if not obj.is_history:
            results.append(check_one_current(spark, table, obj, scope))
        results += check_pii(spark, table, obj, scope)
        if full and obj.is_history:
            results += check_history_fields(spark, catalog, table, obj)
    results += check_run_log(spark, catalog, run_id, {o.name: o for o in objects})
    results += check_pending_approvals(spark, catalog)
    results += check_freshness(spark, catalog, objects, env, freshness_hours)
    return results
