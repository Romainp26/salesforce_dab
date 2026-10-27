import base64
from dataclasses import replace

import pytest
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from pyspark.sql import functions as F

from sf_ingest.config import ObjectConfig
from sf_ingest.encrypt import (
    CipherSpec,
    condition_fields,
    decrypt_change,
    encrypt_change,
    encrypt_columns,
    is_ciphertext_sql,
    pii_predicates,
    pin_base64_format,
    raw_text_sql,
)

from .conftest import IV, IV_B64, KEY, KEY_B64, LEGACY_IV_HEX, LEGACY_KEY_TEXT, PII, account_config

pytestmark = pytest.mark.spark


def reference_ciphertext(
    plaintext: str, key: bytes = KEY, iv: bytes = IV, iv_prefixed: bool = True, line_breaks: bool = False
) -> str:
    """AES-CBC/PKCS7 with a static IV, as base64 of IV + ciphertext (Spark's native output).

    `line_breaks` adds CRLF every 76 characters, as Spark's base64() can.
    """
    padder = padding.PKCS7(128).padder()
    data = padder.update(plaintext.encode()) + padder.finalize()
    enc = Cipher(algorithms.AES(key), modes.CBC(iv)).encryptor()
    cipher = enc.update(data) + enc.finalize()
    text = base64.b64encode(iv + cipher if iv_prefixed else cipher).decode()
    return "\r\n".join(text[i : i + 76] for i in range(0, len(text), 76)) if line_breaks else text


def legacy_ciphertext(plaintext: str, iv_prefixed: bool = True, line_breaks: bool = False) -> str:
    """The backup_salesforce format: the legacy UTF-8 key and hex IV."""
    key, iv = LEGACY_KEY_TEXT.encode(), bytes.fromhex(LEGACY_IV_HEX)
    return reference_ciphertext(plaintext, key, iv, iv_prefixed, line_breaks)


def test_spark_output_is_byte_identical_to_reference(spark, spec):
    values = ["john@x.com", "John@x.com", "0812345678", "ไทย ชื่อ", "a" * 41]
    df = spark.createDataFrame([(v,) for v in values], "v STRING")
    got = [r.c for r in df.select(F.expr(spec.encrypt_sql("v")).alias("c")).collect()]
    assert got == [reference_ciphertext(v) for v in values]
    # Deterministic, and case-sensitive: no normalisation.
    assert got[0] != got[1]
    assert "\n" not in got[4]  # 88 characters, one line


def test_base64_line_breaks_are_pinned(spark, spec):
    long = "a" * 41  # 48 bytes of ciphertext + 16 of IV: 88 base64 characters
    df = spark.createDataFrame([(long,)], "v STRING")
    try:
        for breaks in (True, False):
            pin_base64_format(spark, replace(PII, base64_line_breaks=breaks))
            got = df.select(F.expr(spec.encrypt_sql("v"))).first()[0]
            assert got == reference_ciphertext(long, line_breaks=breaks)
            # Either form decrypts and passes the ciphertext check.
            back = df.select(F.expr(spec.decrypt_sql(spec.encrypt_sql("v"))), F.expr(is_ciphertext_sql(f"'{got}'")))
            assert tuple(back.first()) == (long, True)
    finally:
        pin_base64_format(spark, PII)


def test_unprefixed_output_drops_the_iv(spark):
    unprefixed = CipherSpec.from_values(f"'{KEY_B64}'", f"'{IV_B64}'", replace(PII, iv_prefixed=False))
    got = spark.range(1).select(F.expr(unprefixed.encrypt_sql("'john@x.com'"))).first()[0]
    assert got == reference_ciphertext("john@x.com", iv_prefixed=False)


@pytest.mark.parametrize("iv_prefixed", [False, True])
def test_decrypt_round_trip(spark, iv_prefixed):
    spec = CipherSpec.from_values(f"'{KEY_B64}'", f"'{IV_B64}'", replace(PII, iv_prefixed=iv_prefixed))
    row = spark.range(1).select(F.expr(spec.decrypt_sql(spec.encrypt_sql("'0812345678'")))).first()
    assert row[0] == "0812345678"


def test_legacy_ciphertext_decrypts_in_every_stored_form(spark, legacy_spec):
    """backup_salesforce may hold Synapse's or salesforce_dab's format: with or without the IV, with line breaks."""
    long = "somebody.with.a.long.address@example.co.th"
    forms = [
        legacy_ciphertext("0812345678"),
        legacy_ciphertext("0812345678", iv_prefixed=False),
        legacy_ciphertext(long, line_breaks=True),
        legacy_ciphertext(long, iv_prefixed=False, line_breaks=True),
    ]
    df = spark.createDataFrame(
        [(v,) for v in [*forms, reference_ciphertext("wrong key"), "plain text", None]], "v STRING"
    )
    got = [r[0] for r in df.select(F.expr(legacy_spec.decrypt_any_sql("v"))).collect()]
    assert got == ["0812345678", "0812345678", long, long, None, None, None]


def test_nulls_stay_null(spark, spec):
    df = spark.createDataFrame([(None,)], "v STRING")
    assert df.select(F.expr(spec.encrypt_sql("v"))).first()[0] is None


def test_encrypted_and_conditional_columns(spark, spec):
    df = spark.createDataFrame(
        [("1", "Jane Doe", "Customer", "0811"), ("2", "Acme Co", "Business", "0822"), ("3", "Bob", None, None)],
        "Id STRING, Name STRING, Type STRING, Phone__c STRING",
    )
    out = {r.Id: r for r in encrypt_columns(df, account_config(), spec).collect()}
    assert out["1"].Name == reference_ciphertext("Jane Doe")
    assert out["2"].Name == "Acme Co"  # business accounts stay readable
    assert out["3"].Name == reference_ciphertext("Bob")  # empty Type counts as a person account
    assert out["1"].Phone__c == reference_ciphertext("0811")
    assert out["3"].Phone__c is None
    assert out["2"].Type == "Business"


def test_history_encryption_only_on_listed_fields(spark, spec):
    obj = ObjectConfig(name="AccountHistory", watermark="CreatedDate", history_encryption=("PersonEmail",))
    df = spark.createDataFrame(
        [("h1", "PersonEmail", "a@x.com", "b@x.com"), ("h2", "Rating", "Hot", "Cold")],
        "Id STRING, Field STRING, OldValue STRING, NewValue STRING",
    )
    out = {r.Id: r for r in encrypt_columns(df, obj, spec).collect()}
    assert out["h1"].OldValue == reference_ciphertext("a@x.com")
    assert out["h1"].NewValue == reference_ciphertext("b@x.com")
    assert (out["h2"].OldValue, out["h2"].NewValue) == ("Hot", "Cold")


def test_ciphertext_check(spark, spec):
    df = spark.createDataFrame([(reference_ciphertext("x"),), ("John",), ("not base64!",)], "v STRING")
    assert [r[0] for r in df.select(F.expr(is_ciphertext_sql("v"))).collect()] == [True, False, False]


def test_condition_fields_reads_identifiers_outside_string_literals():
    from sf_ingest.config import load_objects

    from .conftest import CONFIG_DIR

    account = {o.name: o for o in load_objects(CONFIG_DIR, "roojai")}["Account"]
    known = ["Id", "Name", "Type", "Industry", "Customer__c"]
    assert condition_fields(account, known) == ["Id", "Type"]
    assert condition_fields(ObjectConfig(name="Contact"), known) == []


def test_conditional_rule_is_skipped_when_its_columns_are_absent(spark, spec):
    df = spark.createDataFrame([("1", "North")], "Id STRING, Region__c STRING")  # no Type: the rule can't run
    assert encrypt_columns(df, account_config(), spec).collect()[0].Region__c == "North"


def test_the_rule_reads_plaintext_even_when_a_column_it_reads_is_encrypted(spark, spec):
    # Config validation refuses this combination (B-26); the order still guarantees the rule sees plaintext.
    obj = replace(account_config(), encrypted_fields=("Type",))
    df = spark.createDataFrame([("1", "Jane", "Customer")], "Id STRING, Name STRING, Type STRING")
    row = encrypt_columns(df, obj, spec).first()
    assert (row.Name, row.Type) == (reference_ciphertext("Jane"), reference_ciphertext("Customer"))


def test_history_rules_skip_history_objects_without_values(spark, spec):
    # OpportunityHistory has no Field/OldValue/NewValue; Opportunity's PII reaches it through the parent (B-21).
    obj = ObjectConfig(name="OpportunityHistory", watermark="CreatedDate", history_encryption=("Mobile__c",))
    df = spark.createDataFrame([("h1", "Prospecting")], "Id STRING, StageName STRING")
    assert encrypt_columns(df, obj, spec).first().StageName == "Prospecting"
    assert pii_predicates(obj, {"id", "stagename"}) == []


def test_raw_text_rebuilds_what_bulk_csv_returns(spark):
    row = (
        spark.sql(
            "SELECT DATE'1990-01-31' AS d, TIMESTAMP_NTZ'2024-05-01 09:00:00' AS ts, true AS b, "
            "CAST(10.50 AS DECIMAL(18,2)) AS m, CAST(1500 AS DECIMAL(18,2)) AS w, 42 AS i"
        )
        .select(
            F.expr(raw_text_sql("d", "date")),
            F.expr(raw_text_sql("ts", "timestamp_ntz")),
            F.expr(raw_text_sql("b", "boolean")),
            F.expr(raw_text_sql("m", "decimal(18,2)")),
            F.expr(raw_text_sql("w", "decimal(18,2)")),
            F.expr(raw_text_sql("i", "int")),
        )
        .first()
    )
    # Datetimes go back to the landed UTC text: a value encrypted in place matches later loads (B-24).
    assert tuple(row) == ("1990-01-31", "2024-05-01T02:00:00.000Z", "true", "10.5", "1500", "42")


def test_decrypt_change_restores_plaintext_and_keeps_other_values(spark, spec):
    df = spark.createDataFrame(
        [(reference_ciphertext("Jane"),), ("Acme Co",), (None,), (reference_ciphertext("x", key=bytes(32)),)],
        "Name STRING",
    )
    changes, where = decrypt_change("Name", spec)
    got = [r[0] for r in df.select(F.expr(changes["Name"])).collect()]
    # A business-account name stays; ciphertext of another key stays too rather than becoming null.
    assert got == ["Jane", "Acme Co", None, reference_ciphertext("x", key=bytes(32))]
    assert where == "`Name` IS NOT NULL"
    assert decrypt_change("history:Phone", spec)[1] == "lower(Field) IN ('phone')"
    # AccountHistory records a Name change as TextName.
    assert decrypt_change("history:Name", spec)[1] == "lower(Field) IN ('name', 'textname')"


def test_encrypt_change_encrypts_the_raw_text_of_typed_columns(spark, spec):
    changes, where = encrypt_change(ObjectConfig(name="Account"), "Birth__c", spec, "date")
    df = spark.sql("SELECT DATE'1990-01-31' AS Birth__c")
    assert df.select(F.expr(changes["Birth__c"])).first()[0] == reference_ciphertext("1990-01-31")
    assert where == "`Birth__c` IS NOT NULL"
