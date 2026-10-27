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
from .encrypt import (
    CipherSpec,
    decrypt_change,
    encrypt_change,
    encrypt_columns,
    has_history_values,
    history_match_sql,
    is_ciphertext_sql,
    key_column,
    key_target,
    pii_keys,
    quote,
    raw_text_sql,
    rotate_change,
)
from .metadata import FieldInfo
from .transform import (
    BANGKOK_OFFSET_HOURS,
    bangkok_now,
    cast_from_string,
    does_not_fit,
    normalize_empty,
    sf_datetime_to_bangkok,
    shift_history_values,
    spark_type,
    type_change,
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
    # Missing from Salesforce altogether (a full load, or the hard-delete check of an incremental object, B-25).
    ("_is_hard_deleted", "BOOLEAN"),
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
# The row hash covers the stored values (ciphertext for PII), never plaintext (B-21). Tables hashed the older way,
# over plaintext, lack this property and are rehashed once.
ROW_HASH_PROPERTY = "sf_ingest.row_hash"
ROW_HASH_VERSION = "stored-v2"
# The commit tag names the table, the change and its sequence number for that key: userMetadata is a session
# setting, so a commit that another object's thread makes while it is set carries the tag too, and a key encrypted,
# decrypted and encrypted again needs a new tag each time.
MIGRATION_TAG = "sf_ingest_pii:{table}:{change}:{key}:{n}"
_MIGRATION_LOCK = threading.Lock()


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


def table_properties(obj: ObjectConfig) -> dict[str, str]:
    props = {"delta.enableDeletionVectors": "true"}
    if not obj.is_history:
        props[ROW_HASH_PROPERTY] = ROW_HASH_VERSION
    return props


def _properties_sql(props: dict[str, str]) -> str:
    return ", ".join(f"{ops.sql_string(k)} = {ops.sql_string(v)}" for k, v in props.items())


# Properties a rewrite doesn't carry over: they follow from the statement itself (features, protocol, clustering
# columns), are kept by Delta or Unity Catalog, which refuse to have them set (row tracking's internal columns,
# in-commit timestamps, catalog-owned commits, the table id), or aren't properties at all (constraints).
_DERIVED_PROPERTIES = (
    "delta.coordinatedCommits.",
    "io.unitycatalog.",
    "delta.enableInCommitTimestamps",
    "delta.inCommitTimestamp",
    "delta.constraints.",
    "delta.columnMapping.maxColumnId",
    "delta.feature.",
    "delta.minReaderVersion",
    "delta.minWriterVersion",
    "delta.rowTracking.",
    "delta.lastUpdateVersion",
    "delta.lastCommitTimestamp",
    "clusteringColumns",
)


def _carried_properties(props: dict[str, str]) -> dict[str, str]:
    return {k: v for k, v in props.items() if not k.startswith(_DERIVED_PROPERTIES)}


def data_files(spark: SparkSession, table: str) -> list[str]:
    """The data files of the table's current version, by name (what a purge must delete once rewritten, B-21)."""
    return sorted({p.rstrip("/").rsplit("/", 1)[-1] for p in spark.table(table).inputFiles()})


def _table_comment(spark: SparkSession, table: str) -> str | None:
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
    return next((r.data_type for r in rows if r.col_name == "Comment" and r.data_type), None)


def ensure_table(
    spark: SparkSession, table: str, columns: list[tuple[str, str]], meta, properties: dict[str, str] | None = None
) -> tuple[dict, bool]:
    """Create the table, or add columns for new fields and new SCD2 columns. Returns ({lower name: type}, created).

    A removed field stays as a column and is null from then on. A retyped field is widened or fails the object
    (`evolve_types`).
    """
    created = False
    if not table_exists(spark, table):
        cols = ", ".join(f"{quote(n)} {t}" for n, t in [*columns, *meta])
        props = properties or {"delta.enableDeletionVectors": "true"}
        spark.sql(f"CREATE TABLE {table} ({cols}) USING DELTA CLUSTER BY (Id) TBLPROPERTIES ({_properties_sql(props)})")
        created = True
    else:
        existing = {f.name.lower() for f in spark.table(table).schema.fields}
        new = [(n, t) for n, t in [*columns, *meta] if n.lower() not in existing]
        if new:
            log.info("%s: adding columns %s", table, [n for n, _ in new])
            spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({', '.join(f'{quote(n)} {t}' for n, t in new)})")
    return stored_types(spark, table), created


def stored_types(spark: SparkSession, table: str) -> dict[str, str]:
    return {f.name.lower(): f.dataType.simpleString() for f in spark.table(table).schema.fields}


def evolve_types(spark: SparkSession, table: str, obj: ObjectConfig, fields: list[FieldInfo], business: list[str]):
    """Fit tonight's columns to Salesforce's current types (B-32): widen a decimal or an integer with Delta type
    widening, keep a type that already holds the values, and fail the object on any other change, which would
    otherwise round, truncate or shift values silently. Returns the column types after the change."""
    stored = stored_types(spark, table)
    wanted = {n.lower(): t for n, t in column_types(obj, fields)}
    widen, incompatible = [], []
    for name in business:
        if name.lower() not in wanted or name.lower() not in stored:
            continue
        change = type_change(stored[name.lower()], wanted[name.lower()])
        if change == "widen":
            widen.append((name, wanted[name.lower()]))
        elif change == "incompatible":
            incompatible.append(f"{name}: {stored[name.lower()]} -> {wanted[name.lower()]}")
    if incompatible:
        raise RuntimeError(
            f"{obj.name}: Salesforce changed these fields' types and the stored values can't take the new ones "
            f"losslessly: {incompatible}. Reset the object (runbook §5.9) or exclude the field"
        )
    if widen:
        log.info("%s: widening %s", table, widen)
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
        for name, new_type in widen:
            spark.sql(f"ALTER TABLE {table} ALTER COLUMN {quote(name)} TYPE {new_type}")
        stored = stored_types(spark, table)
        if not obj.is_history and any(t.upper().startswith("DECIMAL") for _, t in widen):
            # A wider scale writes 10.50 as 10.5000: rehash, or every record would look changed (B-32).
            tracked = tracked_columns(obj, spark.table(table).columns)
            spark.sql(f"UPDATE {table} SET _row_hash = {hash_sql(tracked)}")
    return stored


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


def tracked_columns(obj: ObjectConfig, columns: list[str]) -> list[str]:
    """Business columns whose changes make a new version, in a fixed order."""
    excluded = {c.lower() for c in obj.hash_exclude} | {c.lower() for c in META_COLUMNS}
    return sorted((c for c in columns if c.lower() not in excluded), key=str.lower)


def hash_sql(columns: list[str], overrides: dict[str, str] | None = None, prefix: str = "") -> str:
    """SHA-256 over the stored values of `columns` (ciphertext for PII, typed values otherwise), as SQL (B-21).

    `overrides` replaces a column's value by an expression, so a statement that rewrites a column also rehashes the
    row from the new value. Nulls are left out of the JSON, so a column the extract doesn't return hashes as null.
    """
    overrides = {k.lower(): v for k, v in (overrides or {}).items()}
    parts = ", ".join(f"'{c}', {overrides.get(c.lower(), prefix + quote(c))}" for c in columns)
    return f"sha2(to_json(named_struct({parts})), 256)"


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
        .withColumn("_is_hard_deleted", F.lit(False))
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
    spark: SparkSession,
    table: str,
    staged: DataFrame,
    full_load: bool = False,
    now_bangkok: datetime | None = None,
    rewrite: bool = False,
) -> DataFrame:
    """New Ids, or a newer watermark with a different hash. Older or equal watermarks are replays (NFR-05).

    Two cases also write a version starting tonight, even at an equal watermark:
    - a record flagged as hard-deleted (a full load's absence, or the hard-delete check, B-25) that is back in
      tonight's extract becomes current again, even unchanged;
    - with `rewrite` (a corrective full reload, B-33), a record whose stored values differ from Salesforce's at the
      same watermark (formula drift, a past bad load) gets a correction version.
    The new version starts at its watermark if that is later, otherwise tonight.
    """
    current = (
        spark.table(table)
        .where("_is_current")
        .select(
            F.col("Id").alias("__cur_id"),
            F.col("_valid_from").alias("__cur_from"),
            F.col("_row_hash").alias("__cur_hash"),
            F.col("_is_deleted").alias("__cur_deleted"),
            F.coalesce(F.col("_is_hard_deleted"), F.lit(False)).alias("__cur_hard"),
        )
    )
    joined = staged.join(current, staged["Id"] == current["__cur_id"], "left")
    newer = F.col("__cur_from").isNull() | (F.col("_valid_from") > F.col("__cur_from"))
    differs = ~F.col("_row_hash").eqNullSafe(F.col("__cur_hash"))
    keep = F.col("__cur_id").isNull() | (newer & differs)
    was_missing = F.col("__cur_hard")
    if full_load:
        was_missing = was_missing | F.coalesce(F.col("__cur_deleted"), F.lit(False))
    back = was_missing & ~F.col("_is_deleted")
    corrected = F.col("__cur_id").isNotNull() & ~newer & differs if rewrite else F.lit(False)
    keep = keep | back | corrected
    tonight = F.expr(ntz_literal(now_bangkok or bangkok_now()))
    joined = joined.withColumn(
        "_valid_from", F.when((back | corrected) & ~newer, tonight).otherwise(F.col("_valid_from"))
    )
    return (
        joined.where(keep)
        .withColumn("_has_current", F.col("__cur_id").isNotNull())
        .drop("__cur_id", "__cur_from", "__cur_hash", "__cur_deleted", "__cur_hard")
    )


# --------------------------------------------------------------------------------------------- PII policy changes


def _key_field(key: str) -> str:
    """The field name a key protects, as decrypt_fields lists it."""
    return key.removeprefix("history:").split("@")[0]


def _ntz_from_text_sql(value_sql: str, target: str) -> str:
    """SQL twin of transform.cast_from_string, for statements that retype a decrypted column."""
    if target.upper() == "TIMESTAMP_NTZ":
        naive = f"try_cast(regexp_replace({value_sql}, '(Z|\\\\+0000)$', '') AS TIMESTAMP_NTZ)"
        return f"{naive} + INTERVAL {BANGKOK_OFFSET_HOURS} HOURS"
    if target.upper() == "STRING":
        return value_sql
    return f"try_cast({value_sql} AS {target})"


def check_key_version(spark: SparkSession, catalog: str, obj: ObjectConfig, key_version: int) -> None:
    """Refuse to load under a new key while existing ciphertext still uses an older one: the rotate task re-encrypts
    first (B-27). Mixed keys would break joins and give unchanged records a new version."""
    rows = spark.table(ops.ops_table(catalog, "encryption_state")).where(F.col("object") == obj.name).collect()
    latest: dict[str, tuple] = {}
    for r in sorted(rows, key=lambda r: r.recorded_at):
        latest[r.pii_key] = (r.action or "encrypted", r.key_version)
    stale = sorted(k for k, (action, version) in latest.items() if action == "encrypted" and version != key_version)
    if stale:
        raise RuntimeError(
            f"{obj.name}: {stale} are encrypted with an older key than pii.key_version {key_version}; "
            "run the key rotation job first (runbook §5.12)"
        )


def _history_tags(spark: SparkSession, table: str) -> tuple[set[str], int]:
    rows = spark.sql(f"DESCRIBE HISTORY {table}").select("version", "userMetadata").collect()
    return {r.userMetadata for r in rows if r.userMetadata}, max(r.version for r in rows)


def _tagged(spark: SparkSession, tag: str, statement: str) -> list:
    """Run one rewrite with its commit tag and return its result rows. One at a time: the tag is a session-wide
    setting shared by every thread."""
    with _MIGRATION_LOCK:
        spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", tag)
        try:
            return spark.sql(statement).collect()
        finally:
            spark.conf.unset("spark.databricks.delta.commitInfo.userMetadata")


def _replace_statement(
    table: str,
    obj: ObjectConfig,
    schema: list[tuple[str, str]],
    exprs: dict[str, str],
    properties: dict[str, str],
    comment: str | None = None,
) -> str:
    """CREATE OR REPLACE TABLE ... AS SELECT, for a change of a column's type. Unlike an UPDATE it leaves no
    deletion-vector residue: the old files leave the table at once, and the purge task deletes them."""
    exprs = {k.lower(): v for k, v in exprs.items()}
    select = ", ".join(f"{exprs.get(n.lower(), quote(n))} AS {quote(n)}" for n, _ in schema)
    described = f"COMMENT {ops.sql_string(comment)} " if comment else ""
    return (
        f"CREATE OR REPLACE TABLE {table} USING DELTA CLUSTER BY (Id) {described}"
        f"TBLPROPERTIES ({_properties_sql(properties)}) AS SELECT {select} FROM {table}"
    )


def apply_pii_policy(
    spark: SparkSession,
    catalog: str,
    obj: ObjectConfig,
    table: str,
    fields: list[FieldInfo],
    spec: CipherSpec,
    key_version: int,
    run_id: str = "manual",
) -> list[str]:
    """Bring the stored values in line with the PII config, in every version (B-21, B-24). Returns the changes made.

    - rehash a table still hashed over plaintext (B-21)
    - decrypt keys removed from the config and listed in `decrypt_fields`; a removed key not listed fails the object
    - encrypt keys added to the config, retyping a non-STRING column to STRING (B-24)

    Each change is one tagged commit followed by a state row, so a crash in between never repeats it. The purge of
    the history they leave behind (`tasks.purge`) is requested before the first one, so no crash can lose it.
    A conditional key names its rule (`encrypt.rule_key`): a changed rule encrypts the rows it now covers.
    """
    events = ops.encryption_events(spark, catalog, obj.name)
    wanted = pii_keys(obj)
    targets = {key_target(k) for k in wanted}
    encrypted = {k for k, (action, _) in events.items() if action == "encrypted"}
    to_encrypt = [k for k in wanted if k.lower() not in {e.lower() for e in encrypted}]
    # A key is dropped when nothing in the config protects its column any more; an older rule's key is not.
    dropped = sorted(k for k in encrypted if key_target(k) not in targets)
    allowed = {f.lower() for f in obj.decrypt_fields}
    if refused := [k for k in dropped if _key_field(k).lower() not in allowed]:
        raise RuntimeError(
            f"{obj.name}: {refused} hold ciphertext but are no longer in the PII config. List them in decrypt_fields "
            "to decrypt their existing values (B-24), or put them back"
        )
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    rehash = not obj.is_history and props.get(ROW_HASH_PROPERTY) != ROW_HASH_VERSION
    if not (to_encrypt or dropped or rehash):
        return []

    tags, start_version = _history_tags(spark, table)
    schema = [(f.name, f.dataType.simpleString()) for f in spark.table(table).schema.fields]
    stored = {n.lower(): t for n, t in schema}
    names = {n.lower(): n for n, _ in schema}
    tracked = tracked_columns(obj, [n for n, _ in schema])
    wanted_types = {f.name.lower(): spark_type(f) for f in fields}
    planned = (["rehash"] if rehash else []) + [f"decrypt:{k}" for k in dropped] + [f"encrypt:{k}" for k in to_encrypt]
    reason = ", ".join(planned)
    # Before the rewrite, with its files, so a crash after it can't lose the purge; again after it (below), so a
    # purge that started while it ran doesn't count as covering it.
    ops.request_purge(spark, catalog, obj.name, reason, start_version, run_id, data_files(spark, table))
    done: list[str] = []

    def run(change: str, key: str, statement_for) -> None:
        n = events.get(key, ("", 0))[1] + 1
        tag = MIGRATION_TAG.format(table=obj.table, change=change, key=key, n=n)
        if tag not in tags:
            log.info("%s: %s %s in every version", obj.name, change, key)
            _tagged(spark, tag, statement_for())

    if rehash:
        tag = MIGRATION_TAG.format(table=obj.table, change="rehash", key="_row_hash", n=1)
        if tag not in tags:
            log.info("%s: rehashing every version over stored values", obj.name)
            _tagged(spark, tag, f"UPDATE {table} SET _row_hash = {hash_sql(tracked)}")
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('{ROW_HASH_PROPERTY}' = '{ROW_HASH_VERSION}')")
        done.append("rehash")

    def statement(changes: dict[str, str], where: str, retype: dict[str, str]):
        def build() -> str:
            if retype:
                # Columns change type: rewrite the table, keeping every row (B-24). A value the rule leaves alone
                # keeps the text a nightly load would write: raw Salesforce text into STRING, a cast otherwise.
                exprs = {}
                for col, value in changes.items():
                    target = retype.get(col.lower(), stored[col.lower()])
                    q = quote(names[col.lower()])
                    other = (
                        raw_text_sql(q, stored[col.lower()])
                        if target.upper() == "STRING"
                        else _ntz_from_text_sql(q, target)
                    )
                    exprs[col] = f"CASE WHEN {where} THEN {value} ELSE {other} END"
                if not obj.is_history:
                    exprs["_row_hash"] = hash_sql(tracked, exprs)
                exprs["_pii_key_version"] = f"CASE WHEN {where} THEN {key_version} ELSE _pii_key_version END"
                return _replace_statement(
                    table,
                    obj,
                    [(n, retype.get(n.lower(), t)) for n, t in schema],
                    exprs,
                    _carried_properties(props) | table_properties(obj),
                    _table_comment(spark, table),
                )
            sets = [f"{quote(names[c.lower()])} = {v}" for c, v in changes.items()]
            if not obj.is_history:
                sets.append(f"_row_hash = {hash_sql(tracked, changes)}")
            sets.append(f"_pii_key_version = {key_version}")
            return f"UPDATE {table} SET {', '.join(sets)} WHERE {where}"

        return build

    for key in dropped:
        col = key_column(key)
        if col in stored:
            changes, where = decrypt_change(key, spec)
            target = wanted_types.get(col) if not key.startswith("history:") else None
            retype = {col: target} if target and target.upper() != "STRING" and stored[col] == "string" else {}
            if retype:
                changes = {c: _ntz_from_text_sql(v, target) for c, v in changes.items()}
            run("decrypt", key, statement(changes, where, retype))
        ops.record_encryption(spark, catalog, obj.name, [key], key_version, "decrypted", run_id)
        done.append(f"decrypt:{key}")

    for key in to_encrypt:
        col = key_column(key)
        if col in stored:
            changes, where = encrypt_change(obj, key, spec, stored[col])
            retype = {} if stored[col] == "string" or key.startswith("history:") else {col: "STRING"}
            run("encrypt", key, statement(changes, where, retype))
        ops.record_encryption(spark, catalog, obj.name, [key], key_version, "encrypted", run_id)
        done.append(f"encrypt:{key}")
    ops.request_purge(spark, catalog, obj.name, reason, start_version, run_id)
    return done


def rotate_key(
    spark: SparkSession,
    catalog: str,
    obj: ObjectConfig,
    table: str,
    previous: dict[int, CipherSpec],
    spec: CipherSpec,
    key_version: int,
    run_id: str,
) -> int:
    """Re-encrypt every PII value still under an older key with the current one (B-27). Returns rows rewritten.

    The keys come from ops_salesforce.encryption_state (what is encrypted), not the config: a key waiting to be
    decrypted is rotated too, and one waiting to be encrypted is left to the nightly load, under the new key.
    """
    keys = ops.encryption_state(spark, catalog, obj.name)
    if not keys:
        return 0
    columns = {c.lower() for c in spark.table(table).columns}
    schema = [f.name for f in spark.table(table).schema.fields]
    tracked = tracked_columns(obj, schema)
    tags, start_version = _history_tags(spark, table)
    versions = [r[0] for r in spark.table(table).select("_pii_key_version").distinct().collect() if r[0] is not None]
    todo = sorted(v for v in versions if v != key_version)
    unknown = [v for v in todo if v not in previous]
    if unknown:
        # Rows stamped with a key removed from previous_keys but holding no ciphertext (written before the object
        # had PII): restamp them, nothing to decrypt.
        holds = _holds_ciphertext_sql(keys, columns)
        if missing := [
            v
            for v in unknown
            if not spark.sql(f"SELECT 1 FROM {table} WHERE _pii_key_version = {v} AND ({holds}) LIMIT 1").isEmpty()
        ]:
            raise RuntimeError(f"{obj.name}: rows use key versions {missing}, which pii.previous_keys doesn't list")
        listed = ", ".join(str(v) for v in unknown)
        log.info("%s: key versions %s hold no ciphertext; restamped %s", obj.name, listed, key_version)
        spark.sql(f"UPDATE {table} SET _pii_key_version = {key_version} WHERE _pii_key_version IN ({listed})")
        todo = [v for v in todo if v in previous]
    reason = f"rotate to key version {key_version}"
    if todo:
        # Before the rewrite, so a crash after it can't lose the purge of the old key's ciphertext.
        ops.request_purge(spark, catalog, obj.name, reason, start_version, run_id, data_files(spark, table))
    rewritten = 0
    for version in todo:
        changes = rotate_change(keys, columns, previous[version], spec)
        sets = [f"{quote(c)} = {v}" for c, v in changes.items()]
        if not obj.is_history:
            sets.append(f"_row_hash = {hash_sql(tracked, changes)}")
        sets.append(f"_pii_key_version = {key_version}")
        tag = MIGRATION_TAG.format(table=obj.table, change="rotate", key=f"v{version}", n=key_version)
        if tag not in tags:
            log.info("%s: re-encrypting key version %s with version %s", obj.name, version, key_version)
            rows = _tagged(spark, tag, f"UPDATE {table} SET {', '.join(sets)} WHERE _pii_key_version = {version}")
            rewritten += int(rows[0]["num_affected_rows"]) if rows else 0
    if todo:
        # Again after it, so a purge that started while it ran doesn't count as covering it.
        ops.request_purge(spark, catalog, obj.name, reason, start_version, run_id)
    ops.record_encryption(spark, catalog, obj.name, sorted(keys), key_version, "encrypted", run_id)
    return rewritten


def _holds_ciphertext_sql(keys: set[str], columns: set[str]) -> str:
    """Rows where a column the keys protect holds ciphertext."""
    conds = []
    for key in sorted(keys):
        if key.startswith("history:"):
            if has_history_values(columns):
                values = " OR ".join(is_ciphertext_sql(c) for c in ("OldValue", "NewValue"))
                conds.append(f"({history_match_sql([key.removeprefix('history:')])} AND ({values}))")
        elif key_column(key) in columns:
            conds.append(is_ciphertext_sql(quote(key_column(key))))
    return " OR ".join(conds) or "false"


def prepare_existing(
    spark: SparkSession,
    catalog: str,
    obj: ObjectConfig,
    fields: list[FieldInfo],
    spec: CipherSpec,
    key_version: int,
    run_id: str,
) -> list[str]:
    """What every run does to an existing table before loading, even with nothing extracted (B-21): check the key
    version, then apply PII config changes."""
    table = scd2_table(catalog, obj)
    if not table_exists(spark, table):
        return []
    check_key_version(spark, catalog, obj, key_version)
    return apply_pii_policy(spark, catalog, obj, table, fields, spec, key_version, run_id)


@dataclass
class LoadStats:
    rows_landed: int = 0
    versions_written: int = 0
    soft_deletes: int = 0
    archivals: int = 0
    watermark_utc: datetime | None = None
    pii_changes: list[str] | None = None


class UncastableValues(ValueError):
    """Landed values that don't fit their column type. The message holds counts per column, never values."""


def profile_landed(raw: DataFrame, obj: ObjectConfig, business: list[str], types: dict[str, str]) -> LoadStats:
    """Row count, UTC watermark and the cast check, in one pass over the landed pages.

    A value that can't be cast, or that a decimal cast would round, would silently change, so the object fails
    instead, before anything is merged.
    """
    targets = {c: types[c.lower()] for c in business if c.lower() in types}
    typed = [c for c, t in targets.items() if t.upper() != "STRING"]
    row = raw.agg(
        F.count("*").alias("__n"),
        F.max(utc_watermark(F.col(obj.watermark))).alias("__wm"),
        *[F.sum(does_not_fit(F.col(c), targets[c]).cast("int")).alias(c) for c in typed],
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
    rewrite: bool = False,
) -> LoadStats:
    table = scd2_table(catalog, obj)
    raw = read_landed(spark, path)
    business = [c for c in raw.columns if c not in ("_page_no", "_extracted_at")]

    meta = HISTORY_META if obj.is_history else SCD2_META
    types, created = ensure_table(spark, table, column_types(obj, fields), meta, table_properties(obj))
    pii_changes: list[str] = []
    if created:
        # A new table only ever receives encrypted values, so nothing needs re-encrypting.
        ops.record_encryption(spark, catalog, obj.name, pii_keys(obj), key_version, run_id=run_id)
    else:
        pii_changes = prepare_existing(spark, catalog, obj, fields, spec, key_version, run_id)
        types = evolve_types(spark, table, obj, fields, business)
    stats = profile_landed(raw, obj, business, types)
    stats.pii_changes = pii_changes
    staged = latest_per_id(raw, obj.watermark)

    if obj.is_history:
        staged = shift_history_values(staged)
    else:
        staged = delete_flags(staged, obj, archival_user_ids)
    staged = cast_business(encrypt_columns(staged, obj, spec), business, types)
    if not obj.is_history:
        # Over the stored values, after encryption and casts: the hash reveals nothing the row doesn't (B-21).
        staged = staged.withColumn("_row_hash", F.expr(hash_sql(tracked_columns(obj, business))))
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
    changes = changed_rows(spark, table, staged, obj.load_type == "full", now_bangkok, rewrite).localCheckpoint()
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
    plan: list[FieldInfo] | None = None,
    run_id: str = "manual",
) -> int:
    """Fill `update` columns of the current versions from a `SELECT Id, ...` extract. Returns rows updated.

    For a field that was approved or auto-added after its records were last extracted. Closed versions keep null
    (their historical values are unknowable), the watermark doesn't move, and Ids absent from the table are skipped:
    they arrive complete with the nightly load. `_row_hash` is recomputed from the new values.
    """
    table = scd2_table(catalog, obj)
    if not table_exists(spark, table):
        raise RuntimeError(f"{obj.name}: {table} doesn't exist yet; nothing to backfill")
    raw = read_landed(spark, path)
    business = [c for c in raw.columns if c not in ("_page_no", "_extracted_at")]
    by_name = {f.name.lower(): f for f in fields}
    types, _ = ensure_table(spark, table, column_types(obj, [by_name[c.lower()] for c in business]), SCD2_META)
    # Apply PII config changes first, as the nightly load does, so the backfilled ciphertext is never encrypted twice.
    prepare_existing(spark, catalog, obj, plan or fields, spec, key_version, run_id)
    types = evolve_types(spark, table, obj, fields, business)

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
    assignments = [f"t.{quote(c)} = s.{quote(c)}" for c in update]
    tracked = tracked_columns(obj, spark.table(table).columns)
    assignments.append(f"t._row_hash = {hash_sql(tracked, {c: f's.{quote(c)}' for c in update}, prefix='t.')}")
    metrics = spark.sql(
        f"MERGE INTO {table} t USING {source} s ON t.Id = s.Id AND t._is_current "
        f"WHEN MATCHED THEN UPDATE SET {', '.join(assignments)}"
    ).first()
    updated = metrics.asDict().get("num_updated_rows") if metrics is not None else None
    return int(updated or 0)
