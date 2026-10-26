from datetime import datetime, timedelta

import pytest

from sf_ingest import dq, ops
from sf_ingest.config import ObjectConfig
from sf_ingest.deletes import flag_hard_deletes
from sf_ingest.metadata import FieldInfo
from sf_ingest.scd2 import load_object, scd2_table
from sf_ingest.transform import bangkok_now
from sf_ingest.views import refresh_views, view_name, view_sql

from .conftest import ACCOUNT_FIELDS, ARCHIVAL, account_config, account_row

pytestmark = pytest.mark.spark

USER = ObjectConfig(name="User", load_type="full", include_created_today=True)
USER_FIELDS = [
    FieldInfo("Id", "id"),
    FieldInfo("Username", "string"),
    FieldInfo("CreatedDate", "datetime"),
    FieldInfo("SystemModstamp", "datetime"),
]
USER_HEADER = [f.name for f in USER_FIELDS]


def user_rows(*ids):
    return [[i, f"{i}@x.com", "2024-01-01T00:00:00.000Z", "2024-01-01T00:00:00.000Z"] for i in ids]


def test_hard_deletes_on_full_load_objects(spark, catalog, spec, landing):
    load_object(
        spark, catalog, USER, USER_FIELDS, landing("u1", user_rows("005A", "005B"), USER_HEADER), (), spec, 1, "r1"
    )
    tonight = landing("u2", user_rows("005A"), USER_HEADER)
    now = datetime(2024, 6, 1, 0, 30)
    assert flag_hard_deletes(spark, catalog, USER, tonight, "r2", now) == 1
    # Idempotent: the deleted version is now current and already flagged.
    assert flag_hard_deletes(spark, catalog, USER, tonight, "r2", now) == 0

    rows = spark.sql(f"SELECT * FROM {scd2_table(catalog, USER)} WHERE Id = '005B' ORDER BY _valid_from").collect()
    assert [(r._is_current, r._is_deleted) for r in rows] == [(False, False), (True, True)]
    assert rows[0]._valid_to == rows[1]._valid_from == rows[1]._deleted_at == now
    assert rows[1]._run_id == "r2" and rows[1].Username == "005B@x.com"

    refresh_views(spark, catalog, (USER,))
    assert [r.Id for r in spark.table(view_name(catalog, USER)).collect()] == ["005A"]


def test_a_hard_deleted_record_that_comes_back_is_current_again(spark, catalog, spec, landing):
    """A User missing one night (say, a visibility glitch) and back the next, unchanged, must not stay deleted."""
    load_object(
        spark, catalog, USER, USER_FIELDS, landing("u1", user_rows("005A", "005B"), USER_HEADER), (), spec, 1, "r1"
    )
    flag_hard_deletes(spark, catalog, USER, landing("u2", user_rows("005A"), USER_HEADER), "r2", datetime(2024, 6, 1))
    night3 = datetime(2024, 6, 2, 0, 30)
    back = landing("u3", user_rows("005A", "005B"), USER_HEADER)
    stats = load_object(spark, catalog, USER, USER_FIELDS, back, (), spec, 1, "r3", now_bangkok=night3)
    assert stats.versions_written == 1
    assert flag_hard_deletes(spark, catalog, USER, back, "r3", night3) == 0

    rows = spark.sql(f"SELECT * FROM {scd2_table(catalog, USER)} WHERE Id = '005B' ORDER BY _valid_from").collect()
    assert [(r._is_current, r._is_deleted) for r in rows] == [(False, False), (False, True), (True, False)]
    assert rows[1]._valid_to == rows[2]._valid_from == night3
    refresh_views(spark, catalog, (USER,))
    assert sorted(r.Id for r in spark.table(view_name(catalog, USER)).collect()) == ["005A", "005B"]
    # Unchanged nights after that write nothing.
    assert load_object(spark, catalog, USER, USER_FIELDS, back, (), spec, 1, "r4").versions_written == 0


def test_empty_full_extract_never_flags_everything(spark, catalog, spec, landing):
    load_object(spark, catalog, USER, USER_FIELDS, landing("u1", user_rows("005A"), USER_HEADER), (), spec, 1, "r1")
    with pytest.raises(RuntimeError, match="no rows"):
        flag_hard_deletes(spark, catalog, USER, landing("u2", [], USER_HEADER), "r2", bangkok_now())


def test_view_sql_applies_policies():
    cols = [
        "Id",
        "Name",
        "CreatedDate",
        "_valid_from",
        "_valid_to",
        "_is_current",
        "_is_deleted",
        "_deleted_at",
        "_is_archived",
        "_archived_at",
        "_row_hash",
        "_run_id",
        "_extracted_at",
        "_page_no",
        "_pii_key_version",
    ]
    retain = view_sql("c", ObjectConfig(name="Contact", view_filter="Id <> '8002r000000vRMSAA2'"), cols)
    assert "WHERE _is_current AND to_date(`CreatedDate`) < to_date(convert_timezone('Asia/Bangkok'" in retain
    assert retain.endswith("AND (Id <> '8002r000000vRMSAA2')")
    assert "NOT _is_deleted" not in retain
    selected = retain.split("FROM")[0]
    assert all(c not in selected for c in ("_row_hash", "_pii_key_version", "_is_deleted", "_valid_from"))
    delete = view_sql("c", ObjectConfig(name="Payments__c", delete_policy="delete", include_created_today=True), cols)
    assert "WHERE _is_current AND NOT _is_deleted" in delete and "CreatedDate`) <" not in delete


def test_views_hide_today_and_soft_deletes_but_show_archivals(spark, catalog, spec, landing):
    today_utc = (bangkok_now() - timedelta(hours=7)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    rows = [
        account_row("001A"),
        account_row("001B", deleted="true"),
        account_row("001C", deleted="true", modified_by=ARCHIVAL[0]),
        account_row("001D", created=today_utc, modstamp=today_utc),
    ]
    retain, delete = account_config(), account_config(delete_policy="delete")
    load_object(spark, catalog, retain, ACCOUNT_FIELDS, landing("a1", rows), ARCHIVAL, spec, 1, "r1")

    assert refresh_views(spark, catalog, (retain,)) == ["Account"]
    assert refresh_views(spark, catalog, (retain,)) == []  # unchanged definition: not recreated
    shown = {r.Id: r for r in spark.table(view_name(catalog, retain)).collect()}
    assert sorted(shown) == ["001A", "001B", "001C"]
    # Soft-deleted and archived records show with Salesforce's IsDeleted; the SCD2 flags stay out of the view.
    assert shown["001B"].IsDeleted and shown["001C"].IsDeleted
    assert not any(c.startswith("_") for c in spark.table(view_name(catalog, retain)).columns)

    assert refresh_views(spark, catalog, (delete,)) == ["Account"]
    assert sorted(r.Id for r in spark.table(view_name(catalog, delete)).collect()) == ["001A", "001C"]


def test_dq_checks(spark, catalog, spec, landing):
    obj = account_config()
    load_object(spark, catalog, obj, ACCOUNT_FIELDS, landing("a1", [account_row("001A")]), ARCHIVAL, spec, 1, "r1")
    ops.append_event(spark, catalog, "r1", "Account", "extract", "succeeded", rows_extracted=1, sf_count=2)
    ops.append_event(
        spark,
        catalog,
        "r1",
        "Account",
        "load",
        "succeeded",
        versions_written=1,
        watermark_after=datetime(2024, 5, 1, 2, 0),
    )
    results = {(r["check_name"], r["detail"].split(":")[0]): r for r in dq.run_checks(spark, catalog, "r1", (obj,))}
    assert results[("one_current_row_per_id", "0 Ids with several current rows")]["passed"]
    assert results[("pii_is_ciphertext", "Phone__c")]["passed"]
    assert results[("pii_is_ciphertext", "Name")]["passed"]
    assert not results[("count_matches_soql", "extracted 1, COUNT() 2")]["passed"]
    assert results[("watermark_advanced", "before None, after 2024-05-01 02")]["passed"]

    # Plaintext slipping into a PII column is a critical failure.
    spark.sql(f"UPDATE {scd2_table(catalog, obj)} SET Phone__c = '0812345678'")
    bad = [r for r in dq.check_pii(spark, scd2_table(catalog, obj), obj) if not r["passed"]]
    assert [(r["severity"], r["detail"]) for r in bad] == [("critical", "Phone__c: 1 non-ciphertext values")]

    ops.append_dq(spark, catalog, "r1", list(results.values()))
    assert spark.table(ops.ops_table(catalog, "dq_results")).count() == len(results)


def test_sql_string_round_trips_quotes_and_backslashes(spark):
    for value in ("plain", "O'Brien", "back\\slash", "both\\'mixed", ""):
        assert spark.sql(f"SELECT {ops.sql_string(value)} AS v").first().v == value
