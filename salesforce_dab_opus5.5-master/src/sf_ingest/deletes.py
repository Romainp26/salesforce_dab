"""Hard deletes on full-load objects (User, GroupMember): flag records missing from tonight's extract (D-18)."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import ObjectConfig
from .scd2 import merge_versions, ntz_literal, scd2_table, table_exists

log = logging.getLogger(__name__)


def flag_hard_deletes(
    spark: SparkSession, catalog: str, obj: ObjectConfig, path: Path | str, run_id: str, now_bangkok: datetime
) -> int:
    """Close the current version of every record absent from the full extract and add a deleted version.

    Callers must only pass a complete, successful extract: an empty or partial one would flag everything.
    """
    table = scd2_table(catalog, obj)
    if not table_exists(spark, table):
        return 0
    landed = spark.read.option("header", True).option("multiLine", True).option("escape", '"').csv(str(path))
    landed_ids = landed.select("Id").where("Id IS NOT NULL AND Id <> ''").distinct()
    if landed_ids.isEmpty():
        raise RuntimeError(f"{obj.name}: full extract has no rows; refusing to flag hard deletes")

    now = F.expr(ntz_literal(now_bangkok))
    missing = (
        spark.table(table)
        .where("_is_current AND NOT _is_deleted")
        .join(landed_ids, "Id", "left_anti")
        .withColumn("_valid_from", now)
        .withColumn("_is_deleted", F.lit(True))
        .withColumn("_deleted_at", now)
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_has_current", F.lit(True))
        .localCheckpoint()
    )
    count = missing.count()
    if count:
        log.info("%s: %d records missing from the full extract flagged deleted", obj.name, count)
        merge_versions(spark, table, missing)
    return count
