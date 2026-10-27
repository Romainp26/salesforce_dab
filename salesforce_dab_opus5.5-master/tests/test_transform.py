from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from sf_ingest.metadata import FieldInfo
from sf_ingest.transform import cast_from_string, shift_history_values, spark_type

pytestmark = pytest.mark.spark


def test_spark_types():
    assert spark_type(FieldInfo("A", "currency", precision=18, scale=2)) == "DECIMAL(18,2)"
    assert spark_type(FieldInfo("A", "double", precision=0)) == "DOUBLE"
    assert spark_type(FieldInfo("A", "datetime")) == "TIMESTAMP_NTZ"
    assert spark_type(FieldInfo("A", "date")) == "DATE"
    assert spark_type(FieldInfo("A", "boolean")) == "BOOLEAN"
    assert spark_type(FieldInfo("A", "picklist")) == "STRING"


def test_datetimes_shift_seven_hours_and_ignore_session_time_zone(spark):
    df = spark.createDataFrame([("2024-05-01T20:04:05.000Z",), ("2024-05-01T20:04:05Z",)], "v STRING")
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    try:
        got = [r[0] for r in df.select(cast_from_string(F.col("v"), "TIMESTAMP_NTZ")).collect()]
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
    assert got == [datetime(2024, 5, 2, 3, 4, 5), datetime(2024, 5, 2, 3, 4, 5)]


def test_other_casts(spark):
    df = spark.createDataFrame(
        [("2024-05-01", "true", "12.30", "7", None)], "d STRING, b STRING, n STRING, i STRING, z STRING"
    )
    row = df.select(
        cast_from_string(F.col("d"), "DATE"),
        cast_from_string(F.col("b"), "BOOLEAN"),
        cast_from_string(F.col("n"), "DECIMAL(18,2)"),
        cast_from_string(F.col("i"), "INT"),
        cast_from_string(F.col("z"), "BOOLEAN"),
    ).first()
    assert tuple(row) == (date(2024, 5, 1), True, Decimal("12.30"), 7, None)


def test_history_datetime_values_shift_without_trailing_z(spark):
    df = spark.createDataFrame(
        [
            ("DateTime", "2024-05-01T20:04:05.000Z", None),
            ("Text", "2024-05-01T20:04:05.000Z", "x"),
            ("DateTime", "not a date", "2024-12-31T17:00:00.000Z"),
        ],
        "DataType STRING, OldValue STRING, NewValue STRING",
    )
    rows = shift_history_values(df).collect()
    assert (rows[0].OldValue, rows[0].NewValue) == ("2024-05-02T03:04:05.000", None)
    assert rows[1].OldValue == "2024-05-01T20:04:05.000Z"
    assert (rows[2].OldValue, rows[2].NewValue) == ("not a date", "2025-01-01T00:00:00.000")


def test_history_without_values_is_unchanged(spark):
    df = spark.createDataFrame([("o1", "Prospecting")], "OpportunityId STRING, StageName STRING")
    assert shift_history_values(df).collect() == df.collect()


def test_a_decimal_cast_that_would_round_does_not_fit(spark):
    from sf_ingest.transform import does_not_fit

    df = spark.createDataFrame([("1.2345",), ("1.2",), ("abc",), (None,)], "v STRING")
    got = [r[0] for r in df.select(does_not_fit(F.col("v"), "DECIMAL(18,2)")).collect()]
    # A scale increase in Salesforce must never round silently (B-32).
    assert got == [True, False, True, False]


def test_type_change_rules():
    from sf_ingest.transform import type_change

    assert type_change("decimal(18,2)", "DECIMAL(18,2)") == "keep"
    assert type_change("string", "DECIMAL(18,2)") == "keep"
    assert type_change("decimal(18,4)", "DECIMAL(16,2)") == "keep"  # already wider
    assert type_change("decimal(16,2)", "DECIMAL(18,4)") == "widen"
    assert type_change("decimal(18,2)", "DECIMAL(18,4)") == "incompatible"  # fewer integer digits
    assert type_change("int", "BIGINT") == "widen"
    assert type_change("bigint", "INT") == "keep"
    assert type_change("timestamp_ntz", "DATE") == "incompatible"
    assert type_change("date", "TIMESTAMP_NTZ") == "incompatible"
    assert type_change("decimal(18,2)", "STRING") == "incompatible"
