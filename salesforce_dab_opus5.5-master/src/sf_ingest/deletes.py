"""Hard deletes: records missing from a complete extract of Ids (D-18, B-25).

Full-load objects (User, GroupMember) are checked every night against their full extract. Incremental objects are
checked every `reconcile_days` against an Id-only queryAll extract, which still returns records in the recycle bin,
so a missing Id was hard-deleted or purged.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import ObjectConfig
from .scd2 import SCD2_META, ensure_table, merge_versions, ntz_literal, scd2_table, table_exists

log = logging.getLogger(__name__)

# Below this many missing records, the share guard doesn't apply: a few deletes in a small table are no sign of a
# truncated extract.
SHARE_GUARD_FLOOR = 10


@dataclass
class HardDeletes:
    flagged: int = 0
    restored: int = 0


def _landed_ids(spark: SparkSession, path: Path | str):
    """The Ids in the landed pages, or None when the extract returned no record."""
    if not any(Path(path).glob("*.csv")):
        return None
    landed = spark.read.option("header", True).option("multiLine", True).option("escape", '"').csv(str(path))
    ids = landed.select("Id").where("Id IS NOT NULL AND Id <> ''").distinct()
    return None if ids.isEmpty() else ids


def flag_hard_deletes(
    spark: SparkSession,
    catalog: str,
    obj: ObjectConfig,
    path: Path | str,
    run_id: str,
    now_bangkok: datetime,
    max_share: float | None = None,
    restore: bool = False,
) -> HardDeletes:
    """Close the current version of every record absent from the extract and add a hard-deleted version.

    Callers must only pass a complete, successful extract: an empty or partial one would flag everything. An empty
    one is refused here unless the table has no current record either, or `max_share` is 1 (every record really
    deleted); with `max_share`, so is one that would flag more than that share of the current records (and more than
    SHARE_GUARD_FLOOR).
    An object's `soql_filter` excludes records from the extract, so it applies to the current records too (it must
    then read as Spark SQL, as `Id != '...'` does); when it doesn't, the check is skipped.
    With `restore`, a record flagged earlier that is back in the extract becomes current again (the full-load path
    does this in `scd2.changed_rows`, from the full records).
    """
    table = scd2_table(catalog, obj)
    out = HardDeletes()
    if not table_exists(spark, table):
        return out
    # A table created before B-25 lacks _is_hard_deleted, and a night with nothing extracted never adds it.
    ensure_table(spark, table, [], SCD2_META)
    now = F.expr(ntz_literal(now_bangkok))
    current = spark.table(table).where("_is_current")
    if obj.soql_filter:
        try:
            current = current.where(obj.soql_filter)
            _ = current.schema  # analysed now, also under Spark Connect, which analyses lazily
        except Exception:
            log.warning("%s: soql_filter isn't valid Spark SQL; hard deletes not checked", obj.name)
            return out
    # Archived records leave Salesforce for good once its recycle bin is emptied: they stay archived and shown (D-23).
    alive = current.where(
        "NOT _is_deleted AND NOT coalesce(_is_archived, false) AND NOT coalesce(_is_hard_deleted, false)"
    )
    landed_ids = _landed_ids(spark, path)
    if landed_ids is None:
        if alive.isEmpty():
            return out
        if max_share is None or max_share < 1:
            what = "full extract" if max_share is None else "Id extract"
            hint = (
                ""
                if max_share is None
                else " (if every record was really deleted, set reconcile.max_deleted_share: 1 for one run)"
            )
            raise RuntimeError(f"{obj.name}: the {what} has no rows; refusing to flag hard deletes{hint}")
        landed_ids = spark.createDataFrame([], "Id STRING")
    missing = (
        alive.join(landed_ids, "Id", "left_anti")
        .withColumn("_valid_from", now)
        .withColumn("_is_deleted", F.lit(True))
        .withColumn("_deleted_at", now)
        .withColumn("_is_hard_deleted", F.lit(True))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_has_current", F.lit(True))
        .localCheckpoint()
    )
    out.flagged = missing.count()
    if out.flagged and max_share is not None:
        total = alive.count()
        if out.flagged > SHARE_GUARD_FLOOR and out.flagged > max_share * total:
            raise RuntimeError(
                f"{obj.name}: {out.flagged} of {total} current records are missing from the Id extract, more than "
                f"{max_share:.0%}; refusing to flag them (B-25). Check the integration user's access, then repair"
            )
    if out.flagged:
        log.info("%s: %d records missing from Salesforce flagged hard-deleted", obj.name, out.flagged)
        merge_versions(spark, table, missing)
    if restore:
        back = (
            current.where("coalesce(_is_hard_deleted, false)")
            .join(landed_ids, "Id", "left_semi")
            .withColumn("_valid_from", now)
            .withColumn("_is_deleted", F.lit(False))
            .withColumn("_deleted_at", F.lit(None).cast("timestamp_ntz"))
            .withColumn("_is_hard_deleted", F.lit(False))
            .withColumn("_run_id", F.lit(run_id))
            .withColumn("_has_current", F.lit(True))
            .localCheckpoint()
        )
        out.restored = back.count()
        if out.restored:
            log.info("%s: %d records flagged hard-deleted are back in Salesforce", obj.name, out.restored)
            merge_versions(spark, table, back)
    return out
