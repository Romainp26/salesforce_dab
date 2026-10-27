"""Initial load from `backup_salesforce`, the manual deep clone of the Synapse tables (FR-04, B-14).

Each backup row becomes the first SCD2 version of its record. Backup timestamps are already Bangkok time, so they
load as they are. PII is decrypted with the legacy key (`pii.legacy`) and encrypted again with the current one.
"""

from __future__ import annotations

import logging
from datetime import UTC, timedelta

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from . import dq, ops
from .config import BACKUP_SCHEMA, ObjectConfig
from .encrypt import CipherSpec, encrypt_columns, is_ciphertext_sql, pii_keys, pii_predicates, quote
from .metadata import FieldInfo
from .scd2 import (
    HISTORY_META,
    SCD2_META,
    UncastableValues,
    column_types,
    ensure_table,
    hash_sql,
    ntz_literal,
    scd2_table,
    table_exists,
    table_properties,
    tracked_columns,
)
from .transform import BANGKOK_OFFSET_HOURS, bangkok_now, normalize_empty

log = logging.getLogger(__name__)


def backup_table(catalog: str, obj: ObjectConfig) -> str:
    return f"`{catalog}`.`{BACKUP_SCHEMA}`.`{obj.table}`"


def match_columns(fields: list[FieldInfo], source_columns: list[str]) -> dict[str, str]:
    """Salesforce field -> backup column, matched by name, case-insensitively. Unmatched fields load as null."""
    available = {c.lower(): c for c in source_columns}
    return {f.name: available[f.name.lower()] for f in fields if f.name.lower() in available}


def legacy_to_plaintext(df: DataFrame, obj: ObjectConfig, legacy: CipherSpec) -> DataFrame:
    """Decrypt every PII value under its rule. Values that aren't ciphertext stay as they are.

    A value shaped like ciphertext that the legacy key can't decrypt fails the object, with counts per column.
    """
    present = {c.lower(): c for c in df.columns}
    checks, plain = [], {}
    for col, applies in pii_predicates(obj, set(present)):
        name = present[col.lower()]
        value = F.col(name).cast("string")
        candidate = F.expr(f"coalesce(({applies}), false)") & F.expr(is_ciphertext_sql(quote(name)))
        decrypted = F.expr(legacy.decrypt_any_sql(f"CAST({quote(name)} AS STRING)"))
        plain[name] = F.when(candidate, decrypted).otherwise(value)
        checks.append(F.sum((candidate & decrypted.isNull()).cast("int")).alias(name))
    if not checks:
        return df
    row = df.agg(*checks).first()
    bad = {c: row[c] for c in plain if row[c]}
    if bad:
        raise UncastableValues(
            f"{obj.name}: PII values that don't decrypt with the legacy key, count per column: {bad}. "
            "Check pii.legacy in entities.yml."
        )
    return df.select(*[plain[c].alias(c) if c in plain else F.col(c) for c in df.columns])


def to_first_versions(
    df: DataFrame,
    obj: ObjectConfig,
    fields: list[FieldInfo],
    mapping: dict[str, str],
    types: dict[str, str],
    archival_user_ids: tuple[str, ...],
    legacy: CipherSpec,
    spec: CipherSpec,
    key_version: int,
    run_id: str,
) -> DataFrame:
    """Backup rows -> first versions: plaintext PII, typed, flagged, hashed, encrypted with the current key."""
    names = [f.name for f in fields]
    out = df.select(*[(F.col(mapping[n]) if n in mapping else F.lit(None)).alias(n) for n in names])
    # As in the nightly load, where Bulk CSV can't tell null from an empty string: both become null (B-38).
    out = normalize_empty(out, [n for n, t in out.dtypes if t == "string"])
    out = legacy_to_plaintext(out, obj, legacy)
    # A legacy ciphertext of '' decrypts to '': null too.
    out = normalize_empty(out, [n for n, t in out.dtypes if t == "string"])

    # Backup timestamps are TIMESTAMP_NTZ or TIMESTAMP holding Bangkok wall-clock time; under the job's UTC session
    # both cast to NTZ without shifting.
    target = {n: types[n.lower()] for n in names}
    checks = [
        F.sum((F.col(n).isNotNull() & F.col(n).try_cast(t).isNull()).cast("int")).alias(n) for n, t in target.items()
    ]
    bad = {n: c for n, c in out.agg(*checks).first().asDict().items() if c}
    if bad:
        raise UncastableValues(
            f"{obj.name}: backup values that don't fit their column type, count per column: {bad}. "
            "Values are withheld because they can hold PII."
        )
    out = out.select(*[F.col(n).try_cast(t).alias(n) for n, t in target.items()])

    meta = {
        "_run_id": F.lit(f"seed-{run_id}"),
        "_extracted_at": F.expr(ntz_literal(bangkok_now())),
        "_page_no": F.lit(None).cast("int"),
        "_pii_key_version": F.lit(key_version),
    }
    if obj.is_history:
        out = encrypt_columns(out.dropDuplicates(["Id"]), obj, spec)
        return out.select(*names, *[v.alias(k) for k, v in meta.items()])

    lower = {n.lower(): n for n in names}
    deleted = F.coalesce(F.col(lower["isdeleted"]), F.lit(False)) if "isdeleted" in lower else F.lit(False)
    archived = (
        deleted & F.coalesce(F.col(lower["lastmodifiedbyid"]).isin(list(archival_user_ids)), F.lit(False))
        if "lastmodifiedbyid" in lower and archival_user_ids
        else F.lit(False)
    )
    valid_from = F.col(lower[obj.watermark.lower()])
    latest = Window.partitionBy("Id").orderBy(valid_from.desc())
    out = (
        out.withColumn("__rn", F.row_number().over(latest))
        .where("__rn = 1")
        .drop("__rn")
        .withColumn("_valid_from", valid_from)
        .withColumn("_valid_to", F.lit(None).cast("timestamp_ntz"))
        .withColumn("_is_current", F.lit(True))
        .withColumn("_is_archived", archived)
        .withColumn("_is_deleted", deleted & ~F.col("_is_archived"))
        .withColumn("_deleted_at", F.when(F.col("_is_deleted"), valid_from))
        .withColumn("_archived_at", F.when(F.col("_is_archived"), valid_from))
        .withColumn("_is_hard_deleted", F.lit(False))
    )
    # Over the stored values (typed, PII encrypted), exactly as the nightly load hashes them (B-21, B-38).
    out = encrypt_columns(out, obj, spec).withColumn("_row_hash", F.expr(hash_sql(tracked_columns(obj, names))))
    for k, v in meta.items():
        out = out.withColumn(k, v)
    return out


def seed(ctx, names: set[str]) -> None:
    """Load every configured object (or `names`) from backup_salesforce into its empty SCD2 table."""
    spark, catalog, t = ctx.spark, ctx.catalog, ctx.settings.target
    legacy = ctx.legacy_spec
    failed, results = [], []
    for obj in [o for o in ctx.settings.objects if not names or o.name in names]:
        source = backup_table(catalog, obj)
        if not table_exists(spark, source):
            log.warning("%s: no %s; the first nightly run will load it in full", obj.name, source)
            results.append(dq.result("seed_count", obj.name, dq.WARNING, False, f"no {source}"))
            continue
        table = scd2_table(catalog, obj)
        try:
            fields = [FieldInfo(**f) for f in ops.read_run_plan(spark, catalog, ctx.run_id, obj.name)]
            types, created = ensure_table(
                spark,
                table,
                column_types(obj, fields),
                HISTORY_META if obj.is_history else SCD2_META,
                table_properties(obj),
            )
            if not created and not spark.table(table).isEmpty():
                raise RuntimeError("target table is not empty; the seed only loads empty tables")
            src = spark.table(source)
            mapping = match_columns(fields, src.columns)
            if "Id" not in mapping or obj.watermark not in mapping:
                raise RuntimeError(f"{source} lacks Id or {obj.watermark}")
            versions = to_first_versions(
                src, obj, fields, mapping, types, t.archival_user_ids, legacy, ctx.spec, t.pii.key_version, ctx.run_id
            ).localCheckpoint()
            # Everything stored is encrypted with the current key: nothing to re-encrypt later. Recorded before the
            # append, so a failure in between can never lead the nightly load to encrypt the ciphertext again.
            done = ops.encryption_state(spark, catalog, obj.name)
            ops.record_encryption(
                spark, catalog, obj.name, [k for k in pii_keys(obj) if k not in done], t.pii.key_version
            )
            versions.write.format("delta").mode("append").saveAsTable(table.replace("`", ""))

            source_count, loaded = src.count(), spark.table(table).count()
            missing = sorted(f.name for f in fields if f.name not in mapping)
            results.append(
                dq.result(
                    "seed_count",
                    obj.name,
                    dq.WARNING,
                    source_count == loaded,
                    f"backup {source_count}, loaded {loaded}, fields missing in the backup: {len(missing)}",
                )
            )
            # Backup timestamps are Bangkok time; watermarks are UTC.
            top = versions.agg(F.max(obj.watermark)).first()[0]
            if top:
                wm = (top - timedelta(hours=BANGKOK_OFFSET_HOURS)).replace(tzinfo=UTC)
                ops.record_watermark(spark, catalog, obj.name, wm, f"seed-{ctx.run_id}")
            log.info(
                "%s: seeded %d rows from %d backup rows, watermark %s UTC; fields not in the backup: %s",
                obj.name,
                loaded,
                source_count,
                f"{wm:%Y-%m-%d %H:%M:%S}" if top else "none",
                ", ".join(missing) or "none",
            )
        except Exception as exc:
            log.exception("%s: seed failed", obj.name)
            results.append(dq.result("seed_count", obj.name, dq.WARNING, False, f"{type(exc).__name__}: {exc}"))
            failed.append(obj.name)
    ops.append_dq(spark, catalog, ctx.run_id, results)
    if failed:
        raise RuntimeError(f"Seed failed for {sorted(failed)}")
