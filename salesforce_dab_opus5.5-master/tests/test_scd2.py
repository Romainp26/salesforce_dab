from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from sf_ingest import ops
from sf_ingest.config import ObjectConfig
from sf_ingest.metadata import FieldInfo
from sf_ingest.scd2 import UncastableValues, apply_new_encryption, load_object, scd2_table

from .conftest import ACCOUNT_FIELDS, ACCOUNT_HEADER, ARCHIVAL, account_config, account_row
from .test_encrypt import reference_ciphertext

pytestmark = pytest.mark.spark


def load(spark, catalog, spec, path, obj=None, fields=ACCOUNT_FIELDS, run_id="r1"):
    return load_object(spark, catalog, obj or account_config(), fields, path, ARCHIVAL, spec, 1, run_id)


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
    plain = account_config(encrypted_fields=())
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

    flagged = account_config()
    table = scd2_table(catalog, flagged)
    assert apply_new_encryption(spark, catalog, flagged, table, spec, 1) == ["Phone__c"]
    assert {r.Phone__c for r in versions(spark, catalog)} == {reference_ciphertext("0812345678")}

    # A crash after the UPDATE but before the state row: the commit tag prevents double encryption.
    spark.sql(f"DELETE FROM {ops.ops_table(catalog, 'encryption_state')} WHERE pii_key = 'Phone__c'")
    apply_new_encryption(spark, catalog, flagged, table, spec, 1)
    assert {r.Phone__c for r in versions(spark, catalog)} == {reference_ciphertext("0812345678")}
    assert apply_new_encryption(spark, catalog, flagged, table, spec, 1) == []


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
