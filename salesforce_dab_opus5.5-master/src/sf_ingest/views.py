"""Policy views in bronze_salesforce (D-14, D-17, D-18, FR-06, FR-07, FR-12)."""

from __future__ import annotations

import hashlib
import logging

from pyspark.sql import SparkSession

from . import ops
from .config import OPS_SCHEMA, VIEW_SCHEMA, ObjectConfig
from .encrypt import quote
from .scd2 import META_COLUMNS, scd2_table, table_exists

log = logging.getLogger(__name__)

# Today's date in Bangkok whatever the reader's session time zone.
BANGKOK_TODAY = "to_date(convert_timezone('Asia/Bangkok', current_timestamp()))"


def complete_until(catalog: str, obj: ObjectConfig) -> str:
    """SQL for the Bangkok date the object's data is complete until: the start date of its latest successful nightly
    ingest, so a failed night never shows a partial day (B-36). Today's date before the first ingest (a seed)."""
    run_log = f"`{catalog}`.`{OPS_SCHEMA}`.`run_log`"
    last = (
        f"SELECT to_date(convert_timezone('Asia/Bangkok', max(started_at))) FROM {run_log} "
        f"WHERE object = {ops.sql_string(obj.name)} AND status = 'succeeded' AND last_stage = 'ingest'"
    )
    return f"coalesce(({last}), {BANGKOK_TODAY})"


def view_name(catalog: str, obj: ObjectConfig) -> str:
    return f"`{catalog}`.`{VIEW_SCHEMA}`.`{obj.table}`"


def view_sql(catalog: str, obj: ObjectConfig, table_columns: list[str]) -> str:
    # Business columns only: the SCD2 columns stay internal. Salesforce's own IsDeleted marks soft-deleted and
    # archived records in `retain` views (B-15).
    select = [c for c in table_columns if c not in META_COLUMNS]
    lower = {c.lower(): c for c in table_columns}
    conds = []
    if obj.is_history:
        if obj.delete_policy == "delete" and "isdeleted" in lower:
            conds.append(f"NOT coalesce({quote(lower['isdeleted'])}, false)")
    else:
        conds.append("_is_current")
        # Hard-deleted records no longer exist in Salesforce: every view hides them (D-18, B-25).
        if obj.delete_policy == "delete" or obj.load_type == "full":
            conds.append("NOT _is_deleted")
        if "_is_hard_deleted" in lower:
            conds.append("NOT coalesce(_is_hard_deleted, false)")
    if not obj.include_created_today and "createddate" in lower:
        # The view stops at the end of the day before the latest successful load, in Bangkok time.
        conds.append(f"to_date({quote(lower['createddate'])}) < {complete_until(catalog, obj)}")
    if obj.view_filter:
        conds.append(f"({obj.view_filter})")
    where = f"\nWHERE {' AND '.join(conds)}" if conds else ""
    cols = ",\n  ".join(quote(c) for c in select)
    return (
        f"CREATE OR REPLACE VIEW {view_name(catalog, obj)} AS\nSELECT\n  {cols}\nFROM {scd2_table(catalog, obj)}{where}"
    )


def refresh_views(spark: SparkSession, catalog: str, objects: tuple[ObjectConfig, ...]) -> list[str]:
    """Recreate a view only when its definition (configuration or schema) changed. Returns the objects updated."""
    state_table = ops.ops_table(catalog, "view_state")
    state = {r.object: r.definition_hash for r in spark.table(state_table).collect()}
    updated = []
    for obj in objects:
        table = scd2_table(catalog, obj)
        if obj.retired:
            # A retired object's data is frozen: drop the view so no consumer reads it as current (B-30).
            if table_exists(spark, view_name(catalog, obj)):
                spark.sql(f"DROP VIEW {view_name(catalog, obj)}")
                if table_exists(spark, table):
                    spark.sql(f"COMMENT ON TABLE {table} IS 'Retired: no longer loaded from Salesforce'")
                updated.append(obj.name)
            continue
        if not table_exists(spark, table):
            continue
        sql = view_sql(catalog, obj, spark.table(table).columns)
        # The column types are part of the definition: a view is recreated after a retype or a widening.
        digest = hashlib.sha256((sql + str(spark.table(table).dtypes)).encode()).hexdigest()
        if state.get(obj.name) == digest and table_exists(spark, view_name(catalog, obj)):
            continue
        spark.sql(sql)
        spark.sql(
            f"MERGE INTO {state_table} t "
            f"USING (SELECT {ops.sql_string(obj.name)} AS object, {ops.sql_string(digest)} AS definition_hash) s "
            "ON t.object = s.object "
            "WHEN MATCHED THEN UPDATE SET definition_hash = s.definition_hash, updated_at = current_timestamp() "
            "WHEN NOT MATCHED THEN INSERT (object, definition_hash, updated_at) "
            "VALUES (s.object, s.definition_hash, current_timestamp())",
        )
        updated.append(obj.name)
    log.info("Views recreated: %s", updated)
    return updated
