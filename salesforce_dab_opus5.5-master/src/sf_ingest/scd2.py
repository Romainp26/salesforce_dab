"""Load landed pages into SCD2 tables with a custom Delta MERGE (D-06, FR-05, NFR-05)."""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import ops
from .config import SCD2_SCHEMA, ObjectConfig
from .encrypt import CipherSpec, encrypt_columns, pii_keys, quote, reencrypt_statement
from .metadata import FieldInfo
from .transform import (
    bangkok_now,
    cast_from_string,
    normalize_empty,
    sf_datetime_to_bangkok,
    shift_history_values,
    spark_type,
    uncastable_counts,
)

log = logging.getLogger(__name__)

SCD2_META = [
    ("_valid_from", "TIMESTAMP_NTZ"),
    ("_valid_to", "TIMESTAMP_NTZ"),
    ("_is_current", "BOOLEAN"),
    ("_is_deleted", "BOOLEAN"),
    ("_deleted_at", "TIMESTAMP_NTZ"),
    ("_is_archived", "BOOLEAN"),
    ("_archived_at", "TIMESTAMP_NTZ"),
    ("_row_hash", "STRING"),
    ("_run_id", "STRING"),
    ("_extracted_at", "TIMESTAMP_NTZ"),
    ("_page_no", "INT"),
    ("_pii_key_version", "INT"),
]
HISTORY_META = [
    ("_run_id", "STRING"),
    ("_extracted_at", "TIMESTAMP_NTZ"),
    ("_page_no", "INT"),
    ("_pii_key_version", "INT"),
]
META_COLUMNS = {name for name, _ in SCD2_META}
# The commit tag names the table: userMetadata is a session setting, so a MERGE that another object's thread
# commits while it is set carries the tag too, and must not count as that object's re-encryption.
REENCRYPT_TAG = "sf_ingest_reencrypt:{table}:{key}"
_REENCRYPT_LOCK = threading.Lock()


def scd2_table(catalog: str, obj: ObjectConfig) -> str:
    return f"`{catalog}`.`{SCD2_SCHEMA}`.`{obj.table}`"


def temp_view_name(prefix: str) -> str:
    return f"__sf_{prefix}_{uuid.uuid4().hex}"


def ntz_literal(value: datetime) -> str:
    return f"TIMESTAMP_NTZ '{value:%Y-%m-%d %H:%M:%S.%f}'"


def column_types(obj: ObjectConfig, fields: list[FieldInfo]) -> list[tuple[str, str]]:
    """Business columns as describe reports them; PII columns hold base64 ciphertext as strings."""
    pii = {c.lower() for c in obj.pii_columns}
    return [(f.name, "STRING" if f.name.lower() in pii else spark_type(f)) for f in fields]


def table_exists(spark: SparkSession, table: str) -> bool:
    return spark.catalog.tableExists(table.replace("`", ""))


def ensure_table(spark: SparkSession, table: str, columns: list[tuple[str, str]], meta) -> tuple[dict, bool]:
    """Create the table, or add columns for new fields. Returns ({lower name: type}, created).

    A retyped field keeps its existing column type (preflight raises the warning); a removed field
    stays as a column and is null from then on.
    """
    created = False
    if not table_exists(spark, table):
        cols = ", ".join(f"{quote(n)} {t}" for n, t in [*columns, *meta])
        spark.sql(
            f"CREATE TABLE {table} ({cols}) USING DELTA CLUSTER BY (Id) "
            "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')"
        )
        created = True
    else:
        existing = {f.name.lower() for f in spark.table(table).schema.fields}
        new = [(n, t) for n, t in columns if n.lower() not in existing]
        if new:
            log.info("%s: adding columns %s", table, [n for n, _ in new])
            spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({', '.join(f'{quote(n)} {t}' for n, t in new)})")
    types = {f.name.lower(): f.dataType.simpleString() for f in spark.table(table).schema.fields}
    return types, created


def read_landed(spark: SparkSession, path: Path | str) -> DataFrame:
    """Read raw pages with every column as a string, plus the page number and extraction time."""
    df = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .option("mode", "FAILFAST")
        .csv(str(path))
    )
    business = df.columns
    page = F.regexp_extract(F.col("_metadata.file_path"), r"page_(\d+)\.csv", 1).cast("int")
    extracted = F.col("_metadata.file_modification_time").cast("timestamp_ntz") + F.expr("INTERVAL 7 HOURS")
    df = df.select(*business, page.alias("_page_no"), extracted.alias("_extracted_at"))
    return normalize_empty(df, business)


def latest_per_id(df: DataFrame, watermark: str) -> DataFrame:
    """Keep one row per Id: overlap windows and chunked queries can return a record twice."""
    w = Window.partitionBy("Id").orderBy(F.col(watermark).desc(), F.col("_page_no").desc())
    return df.withColumn("__rn", F.row_number().over(w)).where("__rn = 1").drop("__rn")


def utc_watermark(col):
    """The raw landed watermark as a UTC TIMESTAMP_NTZ. Watermarks stay in UTC (D-24)."""
    return F.regexp_replace(col, r"(Z|\+0000)$", "").try_cast("timestamp_ntz")


def row_hash(df: DataFrame, obj: ObjectConfig, business: list[str]) -> DataFrame:
    """Hash of tracked plaintext fields, so re-encrypting a column never creates versions."""
    excluded = {c.lower() for c in obj.hash_exclude}
    tracked = sorted((c for c in business if c.lower() not in excluded), key=str.lower)
    return df.withColumn("_row_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in tracked])), 256))


def delete_flags(df: DataFrame, obj: ObjectConfig, archival_user_ids: tuple[str, ...]) -> DataFrame:
    """D-23: a deleted record removed by an archival user is archived, otherwise soft-deleted."""
    cols = {c.lower(): c for c in df.columns}
    deleted = F.lower(F.col(cols["isdeleted"])) == "true" if "isdeleted" in cols else F.lit(False)
    deleted = F.coalesce(deleted, F.lit(False))
    if "lastmodifiedbyid" in cols and archival_user_ids:
        archived = deleted & F.coalesce(F.col(cols["lastmodifiedbyid"]).isin(list(archival_user_ids)), F.lit(False))
    else:
        archived = F.lit(False)
    valid_from = sf_datetime_to_bangkok(F.col(cols[obj.watermark.lower()]))
    return (
        df.withColumn("_valid_from", valid_from)
        .withColumn("_is_archived", archived)
        .withColumn("_is_deleted", deleted & ~F.col("_is_archived"))
        .withColumn("_deleted_at", F.when(F.col("_is_deleted"), F.col("_valid_from")))
        .withColumn("_archived_at", F.when(F.col("_is_archived"), F.col("_valid_from")))
    )


def cast_business(df: DataFrame, business: list[str], types: dict[str, str]) -> DataFrame:
    return df.select(
        *[cast_from_string(F.col(c), types[c.lower()]).alias(c) if c in business else F.col(c) for c in df.columns]
    )


def merge_versions(spark: SparkSession, table: str, changes: DataFrame) -> None:
    """Close the current version and insert the new one for every row in `changes`.

    `changes` holds complete new versions plus `_has_current`. A row with a current version appears twice in
    the source: keyed (to close the current row) and unkeyed (to insert the new one).
    """
    data_cols = [c for c in changes.columns if c != "_has_current"]
    keyed = changes.withColumn("_merge_key", F.col("Id"))
    unkeyed = changes.where("_has_current").withColumn("_merge_key", F.lit(None).cast("string"))
    source = temp_view_name("scd2_source")
    keyed.unionByName(unkeyed).drop("_has_current").createOrReplaceTempView(source)
    cols = ", ".join(quote(c) for c in data_cols)
    vals = ", ".join(f"s.{quote(c)}" for c in data_cols)
    spark.sql(
        f"""
        MERGE INTO {table} t
        USING {source} s
        ON t.Id = s._merge_key AND t._is_current
        WHEN MATCHED THEN UPDATE SET t._is_current = false, t._valid_to = s._valid_from
        WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})
        """
    )


def changed_rows(
    spark: SparkSession, table: str, staged: DataFrame, full_load: bool = False, now_bangkok: datetime | None = None
) -> DataFrame:
    """New Ids, or a newer watermark with a different hash. Older or equal watermarks are replays (NFR-05).

    On full-load objects a record flagged as hard-deleted (D-18) that is back in tonight's extract becomes current
    again, even unchanged: the deleted version's `_valid_from` is the night it went missing, later than the record's
    own watermark. The new version starts at its watermark if that is later, otherwise tonight.
    """
    current = (
        spark.table(table)
        .where("_is_current")
        .select(
            F.col("Id").alias("__cur_id"),
            F.col("_valid_from").alias("__cur_from"),
            F.col("_row_hash").alias("__cur_hash"),
            F.col("_is_deleted").alias("__cur_deleted"),
        )
    )
    joined = staged.join(current, staged["Id"] == current["__cur_id"], "left")
    newer = F.col("__cur_from").isNull() | (F.col("_valid_from") > F.col("__cur_from"))
    keep = F.col("__cur_id").isNull() | (newer & ~F.col("_row_hash").eqNullSafe(F.col("__cur_hash")))
    if full_load:
        back = F.coalesce(F.col("__cur_deleted"), F.lit(False)) & ~F.col("_is_deleted")
        keep = keep | back
        tonight = F.expr(ntz_literal(now_bangkok or bangkok_now()))
        joined = joined.withColumn("_valid_from", F.when(back & ~newer, tonight).otherwise(F.col("_valid_from")))
    return (
        joined.where(keep)
        .withColumn("_has_current", F.col("__cur_id").isNotNull())
        .drop("__cur_id", "__cur_from", "__cur_hash", "__cur_deleted")
    )


def apply_new_encryption(
    spark: SparkSession, catalog: str, obj: ObjectConfig, table: str, spec: CipherSpec, key_version: int
) -> list[str]:
    """Encrypt existing values in every version for PII keys added to the config since the last run.

    Each UPDATE is tagged in the Delta commit, so a crash before the state row is written never leads to
    encrypting the same column twice.
    """
    done = ops.encryption_state(spark, catalog, obj.name)
    todo = [k for k in pii_keys(obj) if k not in done]
    if not todo:
        return []
    columns = {f.name.lower() for f in spark.table(table).schema.fields}
    tags = {r.userMetadata for r in spark.sql(f"DESCRIBE HISTORY {table}").select("userMetadata").collect()}
    for key in todo:
        col = "oldvalue" if key.startswith("history:") else key.lower()
        tag = REENCRYPT_TAG.format(table=obj.table, key=key)
        if col in columns and tag not in tags:
            log.info("%s: encrypting existing values of %s", obj.name, key)
            # One re-encryption at a time: the commit tag is a session-wide setting shared by every thread.
            with _REENCRYPT_LOCK:
                spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", tag)
                try:
                    spark.sql(reencrypt_statement(obj, table, key, spec))
                finally:
                    spark.conf.unset("spark.databricks.delta.commitInfo.userMetadata")
        ops.record_encryption(spark, catalog, obj.name, [key], key_version)
    return todo


@dataclass
class LoadStats:
    rows_landed: int = 0
    versions_written: int = 0
    soft_deletes: int = 0
    archivals: int = 0
    watermark_utc: datetime | None = None


class UncastableValues(ValueError):
    """Landed values that don't fit their column type. The message holds counts per column, never values."""


def profile_landed(raw: DataFrame, obj: ObjectConfig, business: list[str], types: dict[str, str]) -> LoadStats:
    """Row count, UTC watermark and the cast check, in one pass over the landed pages.

    A value that can't be cast would silently become null, so the object fails instead, before anything is merged.
    """
    targets = {c: types[c.lower()] for c in business if c.lower() in types}
    typed = [c for c, t in targets.items() if t.upper() != "STRING"]
    row = raw.agg(
        F.count("*").alias("__n"),
        F.max(utc_watermark(F.col(obj.watermark))).alias("__wm"),
        *[
            F.sum((F.col(c).isNotNull() & cast_from_string(F.col(c), targets[c]).isNull()).cast("int")).alias(c)
            for c in typed
        ],
    ).first()
    bad = {c: row[c] for c in typed if row[c]}
    if bad:
        raise UncastableValues(
            f"{obj.name}: values that don't fit their column type, count per column: {bad}. "
            "Values are withheld because they can hold PII."
        )
    wm = row["__wm"]
    return LoadStats(rows_landed=row["__n"], watermark_utc=wm.replace(tzinfo=UTC) if wm else None)


def load_object(
    spark: SparkSession,
    catalog: str,
    obj: ObjectConfig,
    fields: list[FieldInfo],
    path: Path | str,
    archival_user_ids: tuple[str, ...],
    spec: CipherSpec,
    key_version: int,
    run_id: str,
    now_bangkok: datetime | None = None,
) -> LoadStats:
    table = scd2_table(catalog, obj)
    raw = read_landed(spark, path)
    business = [c for c in raw.columns if c not in ("_page_no", "_extracted_at")]

    meta = HISTORY_META if obj.is_history else SCD2_META
    types, created = ensure_table(spark, table, column_types(obj, fields), meta)
    stats = profile_landed(raw, obj, business, types)
    staged = latest_per_id(raw, obj.watermark)

    if created:
        # A new table only ever receives encrypted values, so nothing needs re-encrypting.
        ops.record_encryption(spark, catalog, obj.name, pii_keys(obj), key_version)
    else:
        apply_new_encryption(spark, catalog, obj, table, spec, key_version)

    if obj.is_history:
        staged = shift_history_values(staged)
    else:
        staged = delete_flags(row_hash(staged, obj, business), obj, archival_user_ids)
    staged = cast_business(encrypt_columns(staged, obj, spec), business, types)
    staged = staged.withColumn("_run_id", F.lit(run_id)).withColumn("_pii_key_version", F.lit(key_version))

    if obj.is_history:
        # History rows never change: append the Ids not stored yet (the overlap brings some back).
        new = staged.join(spark.table(table).select("Id"), "Id", "left_anti").localCheckpoint()
        stats.versions_written = new.count()
        if stats.versions_written:
            source = temp_view_name("history_source")
            new.createOrReplaceTempView(source)
            cols = ", ".join(quote(c) for c in new.columns)
            vals = ", ".join(f"s.{quote(c)}" for c in new.columns)
            spark.sql(
                f"MERGE INTO {table} t USING {source} s ON t.Id = s.Id "
                f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})"
            )
        return stats

    staged = staged.withColumn("_is_current", F.lit(True)).withColumn("_valid_to", F.lit(None).cast("timestamp_ntz"))
    changes = changed_rows(spark, table, staged, obj.load_type == "full", now_bangkok).localCheckpoint()
    agg = changes.agg(
        F.count("*").alias("n"),
        F.sum(F.col("_is_deleted").cast("int")).alias("deleted"),
        F.sum(F.col("_is_archived").cast("int")).alias("archived"),
    ).first()
    stats.versions_written = agg.n
    stats.soft_deletes = agg.deleted or 0
    stats.archivals = agg.archived or 0
    if agg.n:
        merge_versions(spark, table, changes)
    return stats


def backfill_current(
    spark: SparkSession,
    catalog: str,
    obj: ObjectConfig,
    fields: list[FieldInfo],
    path: Path | str,
    update: list[str],
    spec: CipherSpec,
    key_version: int,
) -> int:
    """Fill `update` columns of the current versions from a `SELECT Id, ...` extract. Returns rows updated.

    For a field that was approved or auto-added after its records were last extracted. Closed versions keep null
    (their historical values are unknowable), the watermark doesn't move, and Ids absent from the table are skipped:
    they arrive complete with the nightly load. `_row_hash` isn't recomputed, so a record's next change writes one
    version even if only this field differs.
    """
    table = scd2_table(catalog, obj)
    if not table_exists(spark, table):
        raise RuntimeError(f"{obj.name}: {table} doesn't exist yet; nothing to backfill")
    raw = read_landed(spark, path)
    business = [c for c in raw.columns if c not in ("_page_no", "_extracted_at")]
    by_name = {f.name.lower(): f for f in fields}
    types, _ = ensure_table(spark, table, column_types(obj, [by_name[c.lower()] for c in business]), SCD2_META)
    # Encrypt the existing values of newly listed PII columns first, as the nightly load does, so the backfilled
    # ciphertext is never encrypted twice.
    apply_new_encryption(spark, catalog, obj, table, spec, key_version)

    bad = uncastable_counts(raw, {c: types[c.lower()] for c in business})
    if bad:
        raise UncastableValues(
            f"{obj.name}: values that don't fit their column type, count per column: {bad}. "
            "Values are withheld because they can hold PII."
        )
    staged = raw.dropDuplicates(["Id"])
    staged = cast_business(encrypt_columns(staged, obj, spec), business, types).select("Id", *update)
    source = temp_view_name("backfill_source")
    staged.createOrReplaceTempView(source)
    assignments = ", ".join(f"t.{quote(c)} = s.{quote(c)}" for c in update)
    metrics = spark.sql(
        f"MERGE INTO {table} t USING {source} s ON t.Id = s.Id AND t._is_current "
        f"WHEN MATCHED THEN UPDATE SET {assignments}"
    ).first()
    updated = metrics.asDict().get("num_updated_rows") if metrics is not None else None
    return int(updated or 0)
