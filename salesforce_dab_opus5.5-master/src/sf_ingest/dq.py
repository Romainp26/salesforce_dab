"""Data quality checks (design: Operations). Critical failures fail the run; warnings feed SQL alerts."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from . import ops
from .config import ObjectConfig
from .encrypt import is_ciphertext_sql, pii_predicates, quote
from .scd2 import scd2_table, table_exists

log = logging.getLogger(__name__)

CRITICAL = "critical"
WARNING = "warning"


def result(check: str, obj: str | None, severity: str, passed: bool, detail: str = "") -> dict:
    return {"check_name": check, "object": obj, "severity": severity, "passed": passed, "detail": detail}


def check_one_current(spark: SparkSession, table: str, obj: ObjectConfig) -> dict:
    dupes = (
        spark.sql(
            f"SELECT count(*) AS n FROM (SELECT Id FROM {table} WHERE _is_current GROUP BY Id HAVING count(*) > 1)"
        )
        .first()
        .n
    )
    return result("one_current_row_per_id", obj.name, CRITICAL, dupes == 0, f"{dupes} Ids with several current rows")


def check_pii(spark: SparkSession, table: str, obj: ObjectConfig) -> list[dict]:
    columns = {c.lower() for c in spark.table(table).columns}
    out = []
    for col, applies in pii_predicates(obj, columns):
        bad = (
            spark.sql(
                f"SELECT count(*) AS n FROM {table} "
                f"WHERE {quote(col)} IS NOT NULL AND ({applies}) AND NOT {is_ciphertext_sql(quote(col))}"
            )
            .first()
            .n
        )
        # Counts only: never log or store field values.
        out.append(result("pii_is_ciphertext", obj.name, CRITICAL, bad == 0, f"{col}: {bad} non-ciphertext values"))
    return out


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


def run_checks(spark: SparkSession, catalog: str, run_id: str, objects: tuple[ObjectConfig, ...]) -> list[dict]:
    results = []
    for obj in objects:
        table = scd2_table(catalog, obj)
        if not table_exists(spark, table):
            continue
        if not obj.is_history:
            results.append(check_one_current(spark, table, obj))
        results += check_pii(spark, table, obj)
    results += check_run_log(spark, catalog, run_id, {o.name: o for o in objects})
    results += check_pending_approvals(spark, catalog)
    return results
