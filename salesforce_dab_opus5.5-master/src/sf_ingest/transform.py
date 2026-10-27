"""Turn landed CSV strings into typed columns; every Salesforce datetime moves to Bangkok time (FR-13, D-24)."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .metadata import FieldInfo

BANGKOK_OFFSET_HOURS = 7
_ISO_DATETIME = r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?(Z|\+0000)?$"


def bangkok_now() -> datetime:
    """Current Bangkok wall-clock time, naive, for TIMESTAMP_NTZ columns."""
    return bangkok_time(datetime.now(UTC))


def bangkok_time(utc: datetime) -> datetime:
    """An aware UTC time as Bangkok wall-clock time, naive, for TIMESTAMP_NTZ columns."""
    return utc.astimezone(UTC).replace(tzinfo=None) + timedelta(hours=BANGKOK_OFFSET_HOURS)


def spark_type(f: FieldInfo) -> str:
    t = f.sf_type
    if t == "boolean":
        return "BOOLEAN"
    if t == "int":
        return "INT"
    if t == "long":
        return "BIGINT"
    if t in ("double", "currency", "percent"):
        if 0 < f.precision <= 38:
            return f"DECIMAL({f.precision},{f.scale})"
        return "DOUBLE"
    if t == "date":
        return "DATE"
    if t == "datetime":
        # NTZ reads the same in every session time zone; a plain TIMESTAMP could show the shift twice.
        return "TIMESTAMP_NTZ"
    return "STRING"


def sf_datetime_to_bangkok(col: Column) -> Column:
    """'2024-05-01T03:04:05.000Z' (UTC) -> TIMESTAMP_NTZ 2024-05-01 10:04:05 (Bangkok, no DST); null if unparseable."""
    naive = F.regexp_replace(col, r"(Z|\+0000)$", "").try_cast("timestamp_ntz")
    return naive + F.expr(f"INTERVAL {BANGKOK_OFFSET_HOURS} HOURS")


def normalize_empty(df: DataFrame, columns: list[str]) -> DataFrame:
    """Bulk API CSV can't tell null from an empty string; both become null."""
    return df.select(
        *[F.when(F.col(c) == "", None).otherwise(F.col(c)).alias(c) if c in columns else F.col(c) for c in df.columns]
    )


def cast_from_string(col: Column, target: str) -> Column:
    """Never throws: a value that doesn't fit becomes null, and `uncastable_counts` reports it.

    An ANSI cast error would quote the value, which can be PII, in the error message and the run log.
    """
    target = target.upper()
    if target == "TIMESTAMP_NTZ":
        return sf_datetime_to_bangkok(col)
    if target == "STRING":
        return col
    return col.try_cast(target)


def does_not_fit(col: Column, target: str) -> Column:
    """True for a non-null string that `cast_from_string` turns into null, or that a DECIMAL cast would round
    (a scale increase in Salesforce must never lose digits silently, B-32)."""
    cast = cast_from_string(col, target)
    bad = col.isNotNull() & cast.isNull()
    if target.lower().startswith("decimal"):
        bad = bad | (col.isNotNull() & cast.isNotNull() & (col.try_cast("decimal(38,18)") != cast))
    return bad


def uncastable_counts(df: DataFrame, types: dict[str, str]) -> dict[str, int]:
    """Count, per column, the non-null strings that don't fit their target type. Counts only, never values.

    `types` maps column name to its target type; STRING columns are skipped.
    """
    typed = {c: t for c, t in types.items() if t.upper() != "STRING"}
    if not typed:
        return {}
    checks = [F.sum(does_not_fit(F.col(c), t).cast("int")).alias(c) for c, t in typed.items()]
    row = df.agg(*checks).first()
    return {c: row[c] for c in typed if row[c]}


def shift_history_values(df: DataFrame) -> DataFrame:
    """OldValue/NewValue are text; shift them when DataType = 'DateTime' and drop the trailing Z."""
    cols = {c.lower(): c for c in df.columns}
    if not {"datatype", "oldvalue", "newvalue"} <= cols.keys():
        return df
    for name in (cols["oldvalue"], cols["newvalue"]):
        value = F.col(name)
        shifted = F.date_format(sf_datetime_to_bangkok(value), "yyyy-MM-dd'T'HH:mm:ss.SSS")
        is_dt = (F.col(cols["datatype"]) == "DateTime") & value.rlike(_ISO_DATETIME)
        df = df.withColumn(name, F.when(is_dt, shifted).otherwise(value))
    return df


_DECIMAL = re.compile(r"^decimal\((\d+),\s*(\d+)\)$")
_INTEGERS = ["int", "bigint"]


def type_change(stored: str, wanted: str) -> str:
    """How a column stored as `stored` takes values of Salesforce's current type `wanted` (Spark type names).

    - "keep": the stored type already holds every value (same type, a wider one, or STRING)
    - "widen": Delta type widening makes it hold them (a larger decimal, int -> bigint)
    - "incompatible": no lossless way, e.g. datetime -> date or a scale decrease into a narrower integer part
    """
    stored, wanted = stored.lower().replace(" ", ""), wanted.lower().replace(" ", "")
    if stored == wanted or stored == "string":
        return "keep"
    s, w = _DECIMAL.match(stored), _DECIMAL.match(wanted)
    if s and w:
        (sp, ss), (wp, ws) = (int(x) for x in s.groups()), (int(x) for x in w.groups())
        if ss >= ws and sp - ss >= wp - ws:
            return "keep"
        if ws >= ss and wp - ws >= sp - ss:
            return "widen"
        return "incompatible"
    if stored in _INTEGERS and wanted in _INTEGERS:
        return "keep" if _INTEGERS.index(stored) >= _INTEGERS.index(wanted) else "widen"
    if stored == "double" and (w or wanted in _INTEGERS):
        return "keep"
    return "incompatible"
