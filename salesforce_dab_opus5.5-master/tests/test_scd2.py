from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from sf_ingest import dq, ops
from sf_ingest.config import ObjectConfig
from sf_ingest.encrypt import CipherSpec, decrypt_at_sql
from sf_ingest.metadata import FieldInfo
from sf_ingest.scd2 import (
    ROW_HASH_PROPERTY,
    UncastableValues,
    apply_pii_policy,
    hash_sql,
    load_object,
    read_landed,
    rotate_key,
    scd2_table,
    tracked_columns,
)

from .conftest import ACCOUNT_FIELDS, ACCOUNT_HEADER, ARCHIVAL, PII, account_config, account_row
from .test_encrypt import reference_ciphertext

pytestmark = pytest.mark.spark


def load(spark, catalog, spec, path, obj=None, fields=ACCOUNT_FIELDS, run_id="r1", key_version=1, **kwargs):
    return load_object(
        spark, catalog, obj or account_config(), fields, path, ARCHIVAL, spec, key_version, run_id, **kwargs
    )


def versions(spark, catalog, obj=None):
    obj = obj or account_config()
    order = "Id" if obj.is_history else "Id, _valid_from"
    return spark.sql(f"SELECT * FROM {scd2_table(catalog, obj)} ORDER BY {order}").collect()


def test_first_load_types_encryption_and_flags(spark, catalog, spec, landing):
    path = landing(
        "p1",
        [
            account_row("001A", modstamp="2024-05-01T02:00:00.000Z"),
            account_row(
                "001B",
                name="Big Biz",
                type_="Business",
                phone="",
                amount="",
                visits="",
                close="",
                modstamp="2024-05-01T03:00:00.000Z",
            ),
        ],
    )
    stats = load(spark, catalog, spec, path)

    assert (stats.rows_landed, stats.versions_written) == (2, 2)
    assert stats.watermark_utc == datetime(2024, 5, 1, 3, 0, tzinfo=UTC)
    a, b = versions(spark, catalog)
    assert a.Name == reference_ciphertext("Acme")
    assert a.Phone__c == reference_ciphertext("0812345678")
    assert a.Amount__c == Decimal("10.50") and a.Visits__c == 3 and a.IsDeleted is False
    assert a.CloseDate__c == date(2024, 5, 1)
    assert a.SystemModstamp == datetime(2024, 5, 1, 9, 0)  # +7h, Bangkok
    assert a._valid_from == a.SystemModstamp and a._valid_to is None and a._is_current
    assert (a._is_deleted, a._is_archived, a._run_id, a._pii_key_version, a._page_no) == (False, False, "r1", 1, 1)
    assert b.Name == "Big Biz" and b.Phone__c is None and b.Amount__c is None
    assert dict(spark.table(scd2_table(catalog, account_config())).dtypes)["SystemModstamp"] == "timestamp_ntz"


def test_changes_overlap_and_replays(spark, catalog, spec, landing):
    load(
        spark,
        catalog,
        spec,
        landing(
            "p1",
            [
                account_row("001A", modstamp="2024-05-01T02:00:00.000Z"),
                account_row("001B", modstamp="2024-05-01T02:00:00.000Z"),
            ],
        ),
    )
    night2 = landing(
        "p2",
        [
            account_row("001A", name="Acme Renamed", modstamp="2024-05-02T02:00:00.000Z"),
            # Overlap duplicate of the older version: dropped by the in-batch dedupe.
            account_row("001A", modstamp="2024-05-01T02:00:00.000Z"),
            # Only SystemModstamp and LastViewedDate changed: excluded from the hash, so no new version.
            account_row("001B", viewed="2024-05-02T01:00:00.000Z", modstamp="2024-05-02T02:00:00.000Z"),
            account_row("001C", modstamp="2024-05-02T02:00:00.000Z"),
        ],
    )
    stats = load(spark, catalog, spec, night2, run_id="r2")
    assert stats.versions_written == 2

    rows = versions(spark, catalog)
    a = [r for r in rows if r.Id == "001A"]
    assert len(a) == 2
    assert a[0]._is_current is False and a[0]._valid_to == a[1]._valid_from == datetime(2024, 5, 2, 9, 0)
    assert a[1]._is_current and a[1].Name == reference_ciphertext("Acme Renamed") and a[1]._run_id == "r2"
    assert len([r for r in rows if r.Id == "001B"]) == 1
    assert len([r for r in rows if r.Id == "001C"]) == 1

    # Re-running the same night is a no-op (NFR-05).
    assert load(spark, catalog, spec, night2, run_id="r2").versions_written == 0
    assert len(versions(spark, catalog)) == 4


def test_soft_delete_archival_and_undelete(spark, catalog, spec, landing):
    load(spark, catalog, spec, landing("p1", [account_row("001A"), account_row("001B")]))
    stats = load(
        spark,
        catalog,
        spec,
        landing(
            "p2",
            [
                account_row("001A", deleted="true", modstamp="2024-05-03T00:00:00.000Z"),
                account_row("001B", deleted="true", modified_by=ARCHIVAL[0], modstamp="2024-05-03T00:00:00.000Z"),
            ],
        ),
        run_id="r2",
    )
    assert (stats.soft_deletes, stats.archivals) == (1, 1)
    current = {r.Id: r for r in versions(spark, catalog) if r._is_current}
    assert current["001A"]._is_deleted and not current["001A"]._is_archived
    assert current["001A"]._deleted_at == datetime(2024, 5, 3, 7, 0)
    assert current["001B"]._is_archived and not current["001B"]._is_deleted
    assert current["001B"]._archived_at == datetime(2024, 5, 3, 7, 0)

    load(spark, catalog, spec, landing("p3", [account_row("001A", modstamp="2024-05-04T00:00:00.000Z")]), run_id="r3")
    current = {r.Id: r for r in versions(spark, catalog) if r._is_current}
    assert current["001A"]._is_deleted is False and current["001A"]._deleted_at is None
    assert len([r for r in versions(spark, catalog) if r.Id == "001A"]) == 3


def test_new_field_adds_a_column(spark, catalog, spec, landing):
    load(spark, catalog, spec, landing("p1", [account_row("001A")]))
    fields = [*ACCOUNT_FIELDS, FieldInfo("Region__c", "picklist")]
    path = landing(
        "p2",
        [account_row("001A", modstamp="2024-05-05T00:00:00.000Z") + ["North"]],
        header=[*ACCOUNT_HEADER, "Region__c"],
    )
    assert load(spark, catalog, spec, path, fields=fields, run_id="r2").versions_written == 1
    rows = versions(spark, catalog)
    assert [r.Region__c for r in rows] == [None, "North"]


def test_newly_flagged_pii_is_encrypted_once_in_every_version(spark, catalog, spec, landing):
    plain = account_config(encrypted_fields=(), conditional_encryption=None)
    load(spark, catalog, spec, landing("p1", [account_row("001A")]), obj=plain)
    load(
        spark,
        catalog,
        spec,
        landing("p2", [account_row("001A", name="New", modstamp="2024-05-06T00:00:00.000Z")]),
        obj=plain,
        run_id="r2",
    )
    assert {r.Phone__c for r in versions(spark, catalog)} == {"0812345678"}

    flagged = account_config(conditional_encryption=None)
    table = scd2_table(catalog, flagged)
    assert apply_pii_policy(spark, catalog, flagged, table, ACCOUNT_FIELDS, spec, 1, "r3") == ["encrypt:Phone__c"]
    assert {r.Phone__c for r in versions(spark, catalog)} == {reference_ciphertext("0812345678")}
    # The rehash in the same statement: the hash covers the ciphertext now (B-21).
    tracked = tracked_columns(flagged, spark.table(table).columns)
    assert spark.sql(f"SELECT count(*) FROM {table} WHERE _row_hash <> {hash_sql(tracked)}").first()[0] == 0
    # The old plaintext is still in the history until the purge task runs.
    assert ops.pending_purges(spark, catalog)["Account"]["state"] == "requested"

    # A crash after the UPDATE but before the state row: the commit tag prevents double encryption.
    spark.sql(f"DELETE FROM {ops.ops_table(catalog, 'encryption_state')} WHERE pii_key = 'Phone__c'")
    apply_pii_policy(spark, catalog, flagged, table, ACCOUNT_FIELDS, spec, 1, "r3")
    assert {r.Phone__c for r in versions(spark, catalog)} == {reference_ciphertext("0812345678")}
    assert apply_pii_policy(spark, catalog, flagged, table, ACCOUNT_FIELDS, spec, 1) == []


def test_a_field_leaving_the_pii_config_is_decrypted_only_when_acknowledged(spark, catalog, spec, landing):
    """S1: no mixed column, ever (B-24)."""
    load(spark, catalog, spec, landing("p1", [account_row("001A")]))
    unlisted = account_config(encrypted_fields=())
    later = landing("p2", [account_row("001A", phone="0899999999", modstamp="2024-05-06T00:00:00.000Z")])
    with pytest.raises(RuntimeError, match="decrypt_fields"):
        load(spark, catalog, spec, later, obj=unlisted, run_id="r2")
    assert {r.Phone__c for r in versions(spark, catalog)} == {reference_ciphertext("0812345678")}

    decrypted = account_config(encrypted_fields=(), decrypt_fields=("Phone__c",))
    load(spark, catalog, spec, later, obj=decrypted, run_id="r2")
    assert [r.Phone__c for r in versions(spark, catalog)] == ["0812345678", "0899999999"]
    # Listed again later: every version is encrypted again, the ones written in between included.
    load(spark, catalog, spec, landing("p3", [account_row("001A", modstamp="2024-05-07T00:00:00.000Z")]), run_id="r3")
    assert [r.Phone__c for r in versions(spark, catalog)] == [
        reference_ciphertext(v) for v in ("0812345678", "0899999999", "0812345678")
    ]
    assert all(r["passed"] for r in dq.check_pii(spark, scd2_table(catalog, account_config()), account_config()))


def test_encrypting_a_typed_column_rewrites_it_as_string_ciphertext(spark, catalog, spec, landing):
    """S2 on a DATE column: retyped to STRING and encrypted in every version, matching later loads (B-24)."""
    plain = account_config(encrypted_fields=(), conditional_encryption=None)
    load(spark, catalog, spec, landing("p1", [account_row("001A"), account_row("001B", close="")]), obj=plain)
    table = scd2_table(catalog, plain)
    assert dict(spark.table(table).dtypes)["CloseDate__c"] == "date"

    flagged = account_config(encrypted_fields=("CloseDate__c",), conditional_encryption=None)
    night2 = landing("p2", [account_row("001C", modstamp="2024-05-06T00:00:00.000Z")])
    load(spark, catalog, spec, night2, obj=flagged, run_id="r2")
    assert dict(spark.table(table).dtypes)["CloseDate__c"] == "string"
    rows = {r.Id: r for r in versions(spark, catalog, flagged)}
    # Re-encrypted in place and loaded tonight: same value, same ciphertext.
    assert rows["001A"].CloseDate__c == rows["001C"].CloseDate__c == reference_ciphertext("2024-05-01")
    assert rows["001B"].CloseDate__c is None

    # And back (S1): decrypted and retyped to DATE.
    back = account_config(encrypted_fields=(), conditional_encryption=None, decrypt_fields=("CloseDate__c",))
    load(
        spark,
        catalog,
        spec,
        landing("p3", [account_row("001D", modstamp="2024-05-07T00:00:00.000Z")]),
        obj=back,
        run_id="r3",
    )
    assert dict(spark.table(table).dtypes)["CloseDate__c"] == "date"
    assert {r.Id: r.CloseDate__c for r in versions(spark, catalog, back)}["001A"] == date(2024, 5, 1)


def test_the_row_hash_never_reveals_plaintext(spark, catalog, spec, landing):
    """The hash covers stored values: guessing the phone needs the AES key (B-21)."""
    obj = account_config()
    path = landing("p1", [account_row("001A")])
    load(spark, catalog, spec, path)
    table = scd2_table(catalog, obj)
    row = spark.table(table).first()
    # What the hash used to be: the landed plaintext, which a brute force over phone numbers could match.
    raw = read_landed(spark, path)
    plaintext_hash = raw.select(F.expr(hash_sql(tracked_columns(obj, ACCOUNT_HEADER)))).first()[0]
    assert row._row_hash != plaintext_hash
    stored = spark.table(table).select(F.expr(hash_sql(tracked_columns(obj, ACCOUNT_HEADER)))).first()[0]
    assert row._row_hash == stored
    assert spark.sql(f"SHOW TBLPROPERTIES {table}").where(f"key = '{ROW_HASH_PROPERTY}'").count() == 1


def test_a_table_hashed_over_plaintext_is_rehashed_once(spark, catalog, spec, landing):
    load(spark, catalog, spec, landing("p1", [account_row("001A")]))
    table = scd2_table(catalog, account_config())
    spark.sql(f"ALTER TABLE {table} UNSET TBLPROPERTIES ('{ROW_HASH_PROPERTY}')")
    spark.sql(f"UPDATE {table} SET _row_hash = 'plaintext-era'")
    assert apply_pii_policy(spark, catalog, account_config(), table, ACCOUNT_FIELDS, spec, 1) == ["rehash"]
    assert spark.table(table).first()._row_hash != "plaintext-era"
    assert apply_pii_policy(spark, catalog, account_config(), table, ACCOUNT_FIELDS, spec, 1) == []
    # Unchanged record, newer watermark: the new hash equals tonight's, so no new version.
    same = landing("p2", [account_row("001A", modstamp="2024-05-06T00:00:00.000Z")])
    assert load(spark, catalog, spec, same, run_id="r2").versions_written == 0


def test_rewrite_corrects_values_at_an_unchanged_watermark(spark, catalog, spec, landing):
    """A full reload never rewrites an equal watermark; `rewrite` writes a correction version tonight (B-33)."""
    load(spark, catalog, spec, landing("p1", [account_row("001A", amount="10.50")]))
    drifted = landing("p2", [account_row("001A", amount="99.00")])  # same SystemModstamp: formula drift
    assert load(spark, catalog, spec, drifted, run_id="r2").versions_written == 0
    tonight = datetime(2024, 6, 1, 1, 0)
    assert load(spark, catalog, spec, drifted, run_id="r3", rewrite=True, now_bangkok=tonight).versions_written == 1
    current = [r for r in versions(spark, catalog) if r._is_current]
    assert [(r.Amount__c, r._valid_from) for r in current] == [(Decimal("99.00"), tonight)]
    assert load(spark, catalog, spec, drifted, run_id="r4", rewrite=True).versions_written == 0


def test_numeric_type_changes_widen_or_fail_but_never_round(spark, catalog, spec, landing):
    load(spark, catalog, spec, landing("p1", [account_row("001A")]))
    table = scd2_table(catalog, account_config())
    # More decimals than the column holds: refused, not rounded (B-32).
    with pytest.raises(UncastableValues, match="Amount__c"):
        load(spark, catalog, spec, landing("p2", [account_row("001B", amount="10.555")]), run_id="r2")

    wider = [replace(f, precision=20, scale=4) if f.name == "Amount__c" else f for f in ACCOUNT_FIELDS]
    load(spark, catalog, spec, landing("p3", [account_row("001B", amount="10.555")]), fields=wider, run_id="r3")
    assert dict(spark.table(table).dtypes)["Amount__c"] == "decimal(20,4)"
    assert {r.Id: r.Amount__c for r in versions(spark, catalog)} == {
        "001A": Decimal("10.5000"),
        "001B": Decimal("10.5550"),
    }

    as_datetime = [replace(f, sf_type="datetime") if f.name == "CloseDate__c" else f for f in ACCOUNT_FIELDS]
    with pytest.raises(RuntimeError, match="CloseDate__c: date -> TIMESTAMP_NTZ"):
        load(spark, catalog, spec, landing("p4", [account_row("001C")]), fields=as_datetime, run_id="r4")


def test_key_rotation_re_encrypts_and_guards_the_nightly_load(spark, catalog, spec, landing):
    """B-27: nothing loads under a new key until every value has been re-encrypted with it."""
    load(spark, catalog, spec, landing("p1", [account_row("001A")]))
    new_key = CipherSpec.from_values("'" + "A" * 43 + "='", f"'{IV_B64_2}'", PII)
    with pytest.raises(RuntimeError, match="key rotation job"):
        load(spark, catalog, new_key, landing("p2", [account_row("001B")]), run_id="r2", key_version=2)

    table = scd2_table(catalog, account_config())
    assert rotate_key(spark, catalog, account_config(), table, {1: spec}, new_key, 2, "r3") == 1
    row = spark.table(table).first()
    assert row._pii_key_version == 2
    decrypted = spark.sql(
        f"SELECT {new_key.decrypt_sql('Phone__c')} AS p, {new_key.decrypt_sql('Name')} AS n FROM {table}"
    ).first()
    assert (decrypted.p, decrypted.n) == ("0812345678", "Acme")
    assert (
        load(spark, catalog, new_key, landing("p2", [account_row("001B")]), run_id="r2", key_version=2).versions_written
        == 1
    )
    assert ops.pending_purges(spark, catalog)["Account"]["reasons"] == ["rotate to key version 2"]


def test_decrypt_at_reads_each_row_with_its_key_version(spark, spec):
    """B-27: consumers keep decrypting while a rotation is pending (`ops_salesforce.decrypt_pii_at`)."""
    new_key = CipherSpec.from_values("'" + "A" * 43 + "='", f"'{IV_B64_2}'", PII)
    old, new = spec.encrypt_sql("'old'"), new_key.encrypt_sql("'new'")
    rows = spark.sql(f"SELECT {old} AS v, 1 AS k UNION ALL SELECT {new}, 2")
    rows.createOrReplaceTempView("rotating")
    sql = decrypt_at_sql("v", "k", new_key, {1: spec})
    assert sorted(r.p for r in spark.sql(f"SELECT {sql} AS p FROM rotating").collect()) == ["new", "old"]
    assert decrypt_at_sql("v", "k", new_key, {}) == new_key.decrypt_sql("v")


IV_B64_2 = "BBBBBBBBBBBBBBBBBBBBBA=="
KEY_B64_3 = "C" * 43 + "="


HISTORY_FIELDS = [
    FieldInfo("Id", "id"),
    FieldInfo("AccountId", "reference"),
    FieldInfo("Field", "picklist"),
    FieldInfo("DataType", "picklist"),
    FieldInfo("OldValue", "anyType"),
    FieldInfo("NewValue", "anyType"),
    FieldInfo("IsDeleted", "boolean"),
    FieldInfo("CreatedDate", "datetime"),
]


def test_history_is_append_only_shifted_and_encrypted(spark, catalog, spec, landing):
    obj = ObjectConfig(name="AccountHistory", watermark="CreatedDate", history_encryption=("PersonEmail",))
    header = [f.name for f in HISTORY_FIELDS]
    rows = [
        ["017A", "001A", "PersonEmail", "Text", "a@x.com", "b@x.com", "false", "2024-05-01T00:00:00.000Z"],
        ["017B", "001A", "Renewal__c", "DateTime", "2024-05-01T20:00:00.000Z", "", "false", "2024-05-01T01:00:00.000Z"],
    ]
    stats = load(spark, catalog, spec, landing("h1", rows, header), obj=obj, fields=HISTORY_FIELDS)
    assert stats.versions_written == 2
    assert stats.watermark_utc == datetime(2024, 5, 1, 1, 0, tzinfo=UTC)
    # The overlap brings 017B back with a new row 017C: only 017C is appended.
    more = [rows[1], ["017C", "001A", "Rating", "Text", "Hot", "Cold", "false", "2024-05-02T00:00:00.000Z"]]
    assert load(spark, catalog, spec, landing("h2", more, header), obj=obj, fields=HISTORY_FIELDS).versions_written == 1

    got = {r.Id: r for r in versions(spark, catalog, obj)}
    assert len(got) == 3
    assert got["017A"].OldValue == reference_ciphertext("a@x.com")
    assert got["017B"].OldValue == "2024-05-02T03:00:00.000" and got["017B"].NewValue is None
    assert got["017B"].CreatedDate == datetime(2024, 5, 1, 8, 0)
    assert "_is_current" not in spark.table(scd2_table(catalog, obj)).columns


def test_uncastable_values_fail_the_object_with_counts_only(spark, catalog, spec, landing):
    rows = [
        account_row("001A", amount="12,5", visits="many"),
        account_row("001B", amount="1e999999", modstamp="31/12/2024 10:00"),
        account_row("001C"),
    ]
    with pytest.raises(UncastableValues) as err:
        load(spark, catalog, spec, landing("bad", rows))
    message = str(err.value)
    assert "'Amount__c': 2" in message and "'Visits__c': 1" in message and "'SystemModstamp': 1" in message
    for value in ("12,5", "many", "31/12/2024"):
        assert value not in message
    # Nothing was merged.
    assert spark.table(scd2_table(catalog, account_config())).count() == 0


def test_rotation_follows_the_recorded_state_not_the_config(spark, catalog, spec, landing):
    """A key added to the config but not encrypted yet is left to the nightly load, under the new key; one waiting to
    be decrypted is rotated, so its later decryption works (B-27)."""
    obj = account_config(conditional_encryption=None)  # Phone__c encrypted
    load(spark, catalog, spec, landing("p1", [account_row("001A"), account_row("001B")]), obj=obj)
    table = scd2_table(catalog, obj)
    new_key = CipherSpec.from_values("'" + "A" * 43 + "='", f"'{IV_B64_2}'", PII)
    # Tonight's config also lists Name (still plaintext) and is about to stop encrypting Phone__c.
    pending = account_config(encrypted_fields=("Name",), conditional_encryption=None, decrypt_fields=("Phone__c",))
    assert rotate_key(spark, catalog, pending, table, {1: spec}, new_key, 2, "r2") == 2
    row = spark.sql(f"SELECT Name, {new_key.decrypt_sql('Phone__c')} AS phone FROM {table} WHERE Id = '001A'").first()
    assert (row.Name, row.phone) == ("Acme", "0812345678")
    # The nightly load then encrypts Name and decrypts Phone__c, both with the new key.
    later = landing("p2", [account_row("001C", modstamp="2024-05-06T00:00:00.000Z")])
    load(spark, catalog, new_key, later, obj=pending, run_id="r3", key_version=2)
    rows = {r.Id: r for r in versions(spark, catalog, pending)}
    assert rows["001A"].Phone__c == "0812345678"
    assert spark.sql(f"SELECT {new_key.decrypt_sql('Name')} FROM {table} WHERE Id = '001A'").first()[0] == "Acme"


def test_a_widened_conditional_rule_reaches_existing_rows(spark, catalog, spec, landing):
    """The conditional key names its rule: a broader rule encrypts the existing rows it now covers."""
    rows = [account_row("001A"), account_row("001B", name="Big Biz", type_="Partner")]
    load(spark, catalog, spec, landing("p1", rows))
    assert {r.Id: r.Name for r in versions(spark, catalog)}["001B"] == "Big Biz"
    from sf_ingest.config import ConditionalEncryption

    wider = account_config(
        conditional_encryption=ConditionalEncryption(
            columns=("Name",), when="Type IS NULL OR lower(Type) IN ('customer', 'prospect', 'partner')"
        )
    )
    load(
        spark,
        catalog,
        spec,
        landing("p2", [account_row("001C", modstamp="2024-05-06T00:00:00.000Z")]),
        obj=wider,
        run_id="r2",
    )
    names = {r.Id: r.Name for r in versions(spark, catalog)}
    # Encrypted once: the customer's name, already ciphertext, isn't encrypted again.
    assert names == {n: reference_ciphertext(v) for n, v in (("001A", "Acme"), ("001B", "Big Biz"), ("001C", "Acme"))}


def test_history_fields_match_case_insensitively(spark, catalog, spec, landing):
    obj = ObjectConfig(name="AccountHistory", watermark="CreatedDate", history_encryption=("personemail",))
    header = [f.name for f in HISTORY_FIELDS]
    rows = [["017A", "001A", "PersonEmail", "Text", "a@x.com", "b@x.com", "false", "2024-05-01T00:00:00.000Z"]]
    load(spark, catalog, spec, landing("h1", rows, header), obj=obj, fields=HISTORY_FIELDS)
    got = spark.table(scd2_table(catalog, obj)).first()
    assert (got.OldValue, got.NewValue) == (reference_ciphertext("a@x.com"), reference_ciphertext("b@x.com"))


def test_the_nightly_load_encrypts_history_rows_recorded_under_an_alias(spark, catalog, spec, landing):
    """AccountHistory records a Name change as TextName: the load encrypts it as Name (B-21)."""
    obj = ObjectConfig(name="AccountHistory", watermark="CreatedDate", history_encryption=("Name",))
    header = [f.name for f in HISTORY_FIELDS]
    rows = [
        ["017A", "001A", "TextName", "Text", "Somchai", "Somsak", "false", "2024-05-01T00:00:00.000Z"],
        ["017B", "001A", "Rating", "Text", "Hot", "Cold", "false", "2024-05-01T00:00:00.000Z"],
    ]
    load(spark, catalog, spec, landing("h1", rows, header), obj=obj, fields=HISTORY_FIELDS)
    got = {r.Id: r for r in versions(spark, catalog, obj)}
    assert got["017A"].OldValue == reference_ciphertext("Somchai")
    assert got["017B"].OldValue == "Hot"


def test_rotation_restamps_old_versions_that_hold_no_ciphertext(spark, catalog, spec, landing):
    """B-27: rows stamped with a key no longer listed, but with no ciphertext, don't block the rotation."""
    obj = account_config(conditional_encryption=None)
    load(spark, catalog, spec, landing("p1", [account_row("001A", phone="")]), obj=obj)
    table = scd2_table(catalog, obj)
    new_key = CipherSpec.from_values("'" + "A" * 43 + "='", f"'{IV_B64_2}'", PII)
    assert rotate_key(spark, catalog, obj, table, {}, new_key, 2, "r2") == 0
    assert {r._pii_key_version for r in spark.table(table).collect()} == {2}

    load(spark, catalog, new_key, landing("p2", [account_row("001B")]), obj=obj, run_id="r3", key_version=2)
    spark.sql(f"UPDATE {table} SET _pii_key_version = 1 WHERE Id = '001B'")  # ciphertext under an unlisted key
    with pytest.raises(RuntimeError, match=r"key versions \[1\]"):
        rotate_key(
            spark, catalog, obj, table, {}, CipherSpec.from_values(f"'{KEY_B64_3}'", f"'{IV_B64_2}'", PII), 3, "r4"
        )
