from datetime import datetime, timedelta

import pytest

from sf_ingest import dq, ops
from sf_ingest.config import AccessGroups, ObjectConfig
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
    assert flag_hard_deletes(spark, catalog, USER, tonight, "r2", now).flagged == 1
    # Idempotent: the deleted version is now current and already flagged.
    assert flag_hard_deletes(spark, catalog, USER, tonight, "r2", now).flagged == 0

    rows = spark.sql(f"SELECT * FROM {scd2_table(catalog, USER)} WHERE Id = '005B' ORDER BY _valid_from").collect()
    assert [(r._is_current, r._is_deleted) for r in rows] == [(False, False), (True, True)]
    assert [r._is_hard_deleted for r in rows] == [False, True]
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
    assert flag_hard_deletes(spark, catalog, USER, back, "r3", night3).flagged == 0

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
        "_is_hard_deleted",
        "_row_hash",
        "_run_id",
        "_extracted_at",
        "_page_no",
        "_pii_key_version",
    ]
    retain = view_sql("c", ObjectConfig(name="Contact", view_filter="Id <> '8002r000000vRMSAA2'"), cols)
    assert (
        "WHERE _is_current AND NOT coalesce(_is_hard_deleted, false) AND to_date(`CreatedDate`) < coalesce((" in retain
    )
    # Complete until the latest successful ingest of the object, today before the first one (B-36).
    assert "FROM `c`.`ops_salesforce`.`run_log` WHERE object = 'Contact'" in retain
    assert retain.endswith("AND (Id <> '8002r000000vRMSAA2')")
    assert "NOT _is_deleted" not in retain
    selected = retain.split("FROM")[0]
    assert all(c not in selected for c in ("_row_hash", "_pii_key_version", "_is_deleted", "_is_hard_deleted"))
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


def test_hard_delete_check_of_an_incremental_object(spark, catalog, spec, landing):
    """B-25: Ids missing from the Id-only extract are flagged and hidden; ones that come back are restored."""
    obj = account_config()
    ids = [f"001{i:03d}" for i in range(250)]
    load_object(
        spark, catalog, obj, ACCOUNT_FIELDS, landing("a1", [account_row(i) for i in ids]), ARCHIVAL, spec, 1, "r1"
    )
    header = ["Id"]
    # 1 of 250 missing: under the 5% guard.
    present = landing("ids1", [[i] for i in ids[1:]], header)
    found = flag_hard_deletes(spark, catalog, obj, present, "r2", datetime(2024, 6, 1), max_share=0.05, restore=True)
    assert (found.flagged, found.restored) == (1, 0)
    refresh_views(spark, catalog, (obj,))
    assert ids[0] not in {r.Id for r in spark.table(view_name(catalog, obj)).collect()}

    # A truncated extract (20 of 250 missing) is refused, not flagged.
    truncated = landing("ids2", [[i] for i in ids[20:]], header)
    with pytest.raises(RuntimeError, match="more than 5%"):
        flag_hard_deletes(spark, catalog, obj, truncated, "r3", datetime(2024, 6, 2), max_share=0.05, restore=True)

    everyone = landing("ids3", [[i] for i in ids], header)
    found = flag_hard_deletes(spark, catalog, obj, everyone, "r4", datetime(2024, 6, 3), max_share=0.05, restore=True)
    assert (found.flagged, found.restored) == (0, 1)
    rows = spark.sql(f"SELECT * FROM {scd2_table(catalog, obj)} WHERE Id = '{ids[0]}' ORDER BY _valid_from").collect()
    assert [(r._is_current, r._is_hard_deleted) for r in rows] == [(False, False), (False, True), (True, False)]


def test_views_stop_at_the_last_successful_ingest_and_retired_views_go(spark, catalog, spec, landing):
    yesterday_utc = (bangkok_now() - timedelta(hours=7, days=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    obj = account_config()
    rows = [account_row("001A"), account_row("001B", created=yesterday_utc, modstamp=yesterday_utc)]
    load_object(spark, catalog, obj, ACCOUNT_FIELDS, landing("a1", rows), ARCHIVAL, spec, 1, "r1")
    # The latest successful ingest started two days ago (tonight's failed): yesterday's records aren't complete.
    two_days_ago = datetime.now() - timedelta(days=2)
    ops.append_rows(
        spark,
        catalog,
        "run_events",
        [{"run_id": "r0", "object": "Account", "stage": "ingest", "status": "succeeded", "event_at": two_days_ago}],
    )
    refresh_views(spark, catalog, (obj,))
    assert [r.Id for r in spark.table(view_name(catalog, obj)).collect()] == ["001A"]

    retired = account_config(retired=True)
    assert refresh_views(spark, catalog, (retired,)) == ["Account"]
    assert not spark.catalog.tableExists(view_name(catalog, obj).replace("`", ""))


def test_table_checks_cover_tonights_rows_unless_full(spark, catalog, spec, landing):
    obj = account_config()
    table = scd2_table(catalog, obj)
    load_object(spark, catalog, obj, ACCOUNT_FIELDS, landing("a1", [account_row("001A")]), ARCHIVAL, spec, 1, "r1")
    spark.sql(f"UPDATE {table} SET Phone__c = '0812345678'")  # an old row gone wrong
    load_object(spark, catalog, obj, ACCOUNT_FIELDS, landing("a2", [account_row("001B")]), ARCHIVAL, spec, 1, "r2")

    def failed(results):
        return sorted(r["check_name"] for r in results if not r["passed"] and r["check_name"].startswith("pii"))

    assert failed(dq.run_checks(spark, catalog, "r2", (obj,))) == []  # tonight's rows only
    assert failed(dq.run_checks(spark, catalog, "r2", (obj,), full=True)) == ["pii_is_ciphertext"]
    assert failed(dq.run_checks(spark, catalog, "r2", (obj,), rewritten={"Account"})) == ["pii_is_ciphertext"]


def test_freshness(spark, catalog):
    objs = (account_config(), account_config(name="Lead"), account_config(name="Contract", paused=("uat",)))
    ops.append_rows(
        spark,
        catalog,
        "run_events",
        [{"run_id": "r1", "object": "Account", "stage": "ingest", "status": "succeeded", "event_at": datetime.now()}],
    )
    got = {r["object"]: r for r in dq.check_freshness(spark, catalog, objs, "uat", 36)}
    assert got["Account"]["passed"] and not got["Lead"]["passed"] and "Contract" not in got


def test_the_hard_delete_check_keeps_archived_records(spark, catalog, spec, landing):
    """D-23: an archived record that Salesforce has emptied from its recycle bin stays archived and shown."""
    obj = account_config()
    rows = [account_row(f"001{i:02d}") for i in range(30)]
    rows[0] = account_row("00100", deleted="true", modified_by=ARCHIVAL[0])
    load_object(spark, catalog, obj, ACCOUNT_FIELDS, landing("a1", rows), ARCHIVAL, spec, 1, "r1")
    present = landing("ids", [[f"001{i:02d}"] for i in range(1, 30)], ["Id"])
    assert flag_hard_deletes(spark, catalog, obj, present, "r2", datetime(2024, 6, 1), 0.05, restore=True).flagged == 0
    refresh_views(spark, catalog, (obj,))
    assert "00100" in {r.Id for r in spark.table(view_name(catalog, obj)).collect()}


def test_a_purge_request_made_while_a_purge_ran_stays_pending(spark, catalog):
    t0 = datetime(2026, 11, 1, 0, 0)
    rows = [
        {"object": "Account", "action": "requested", "version": 3, "event_at": t0},
        {"object": "Account", "action": "requested", "version": 7, "event_at": t0 + timedelta(minutes=10)},
        {
            "object": "Account",
            "action": "reorged",
            "version": 9,
            "event_at": t0 + timedelta(minutes=20),
            "started_at": t0 + timedelta(minutes=5),
            "files": '["a.parquet"]',
        },
    ]
    ops.append_rows(spark, catalog, "pii_purge", rows)
    pending = ops.pending_purges(spark, catalog)["Account"]
    assert pending["state"] == "requested" and pending["files"] == ["a.parquet"]
    # Covered by the next REORG, then VACUUM, then a rewrite after it starts over from the rewrite's version.
    later = [
        {
            "object": "Account",
            "action": "reorged",
            "version": 11,
            "event_at": t0 + timedelta(days=1),
            "started_at": t0 + timedelta(days=1, minutes=-5),
            "files": '["a.parquet", "b.parquet"]',
        },
        {"object": "Account", "action": "files_purged", "version": 12, "event_at": t0 + timedelta(days=2)},
        {"object": "Account", "action": "requested", "version": 13, "event_at": t0 + timedelta(days=2, hours=1)},
    ]
    ops.append_rows(spark, catalog, "pii_purge", later[:2])
    assert ops.pending_purges(spark, catalog)["Account"]["state"] == "files_purged"
    ops.append_rows(spark, catalog, "pii_purge", later[2:])
    assert ops.pending_purges(spark, catalog)["Account"] | {"reasons": None, "at": None, "requested_at": None} == {
        "state": "requested",
        "version": 13,
        "files": [],
        "reasons": None,
        "at": None,
        "requested_at": None,
    }


def test_grants_cover_every_group_and_never_the_landing_volume():
    access = AccessGroups("readers", "history", "operators", "pii")
    grants = ops.grant_statements("cat", access)
    assert ("GRANT SELECT ON SCHEMA `cat`.`bronze_salesforce`", "readers") in grants
    assert ("GRANT SELECT ON SCHEMA `cat`.`bronze_salesforce_scd2`", "history") in grants
    assert ("GRANT EXECUTE ON FUNCTION `cat`.`ops_salesforce`.`decrypt_pii_at`", "pii") in grants
    assert not any("VOLUME" in g or "MODIFY" in g for g, _ in grants)
    assert not any(group == "pii" and "SELECT" in g for g, group in grants)


def test_a_few_hard_deletes_in_a_small_table_pass_the_share_guard(spark, catalog, spec, landing):
    """B-25: 3 of 20 is over 5%, but no more than SHARE_GUARD_FLOOR records: real deletes, not a truncated extract."""
    obj = account_config()
    ids = [f"001{i:02d}" for i in range(20)]
    load_object(
        spark, catalog, obj, ACCOUNT_FIELDS, landing("a1", [account_row(i) for i in ids]), ARCHIVAL, spec, 1, "r1"
    )
    present = landing("ids", [[i] for i in ids[3:]], ["Id"])
    assert flag_hard_deletes(spark, catalog, obj, present, "r2", datetime(2024, 6, 1), max_share=0.05).flagged == 3


def test_an_empty_id_extract_is_refused_unless_nothing_is_current(spark, catalog, spec, landing, tmp_path):
    obj = account_config()
    empty = tmp_path / "no_pages"
    empty.mkdir()
    load_object(spark, catalog, obj, ACCOUNT_FIELDS, landing("a1", [account_row("001A")]), ARCHIVAL, spec, 1, "r1")
    with pytest.raises(RuntimeError, match="no rows"):
        flag_hard_deletes(spark, catalog, obj, empty, "r2", datetime(2024, 6, 1), max_share=0.05)
    # Every record really hard-deleted in Salesforce: accepted for one run with max_deleted_share 1.
    assert flag_hard_deletes(spark, catalog, obj, empty, "r3", datetime(2024, 6, 1), max_share=1).flagged == 1
    # Then no record is left: an empty extract has nothing to check.
    assert flag_hard_deletes(spark, catalog, obj, empty, "r4", datetime(2024, 6, 2), max_share=0.05).flagged == 0


def test_history_field_values_that_match_no_parent_field_are_reported(spark, catalog):
    """B-21: a PII field recorded under another name in a History table would stay plaintext."""
    history = ObjectConfig(name="AccountHistory", watermark="CreatedDate", history_encryption=("Phone",))
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`bronze_salesforce_scd2`")
    table = scd2_table(catalog, history)
    rows = [("1", "Phone"), ("2", "TextName"), ("3", "created"), ("4", "Mystery__c")]
    spark.createDataFrame(rows, "Id STRING, Field STRING").selectExpr(
        "*", "'x' AS OldValue", "'y' AS NewValue"
    ).write.saveAsTable(table)
    ops.append_rows(spark, catalog, "object_schema", [{"object": "Account", "field": f} for f in ("Phone", "Name")])
    [found] = dq.check_history_fields(spark, catalog, table, history)
    assert (found["check_name"], found["passed"]) == ("history_field_unmapped", False)
    assert "['mystery__c']" in found["detail"]


def test_records_the_soql_filter_excludes_are_not_flagged_hard_deleted(spark, catalog, spec, landing):
    """B-25: a record seeded from the backup that the Id extract's soql_filter leaves out (Contract's corrupted one)."""
    obj = account_config(soql_filter="Id != '001A'")
    rows = [account_row("001A"), account_row("001B")]
    load_object(spark, catalog, obj, ACCOUNT_FIELDS, landing("a1", rows), ARCHIVAL, spec, 1, "r1")
    present = landing("ids", [["001B"]], ["Id"])
    assert flag_hard_deletes(spark, catalog, obj, present, "r2", datetime(2024, 6, 1), max_share=0.05).flagged == 0


def test_a_purge_step_decided_before_another_jobs_request_does_not_cover_it(spark, catalog):
    """B-21: a backfill requests a purge while the nightly purge, working from an earlier read, finishes one."""
    t0 = datetime(2026, 11, 1, 0, 0)
    rows = [
        {"object": "Account", "action": "requested", "version": 3, "event_at": t0},
        {
            "object": "Account",
            "action": "reorged",
            "version": 4,
            "event_at": t0 + timedelta(hours=1),
            "started_at": t0 + timedelta(minutes=30),
            "files": "[]",
        },
        {
            "object": "Account",
            "action": "files_purged",
            "version": 5,
            "event_at": t0 + timedelta(days=1),
            "started_at": t0 + timedelta(days=1),
        },
        # The backfill's request, then the nightly purge's done from its read before it.
        {
            "object": "Account",
            "action": "requested",
            "version": 8,
            "event_at": t0 + timedelta(days=2, minutes=10),
            "files": '["c.parquet"]',
        },
        {
            "object": "Account",
            "action": "done",
            "version": 9,
            "event_at": t0 + timedelta(days=2, minutes=20),
            "started_at": t0 + timedelta(days=2),
        },
    ]
    ops.append_rows(spark, catalog, "pii_purge", rows)
    pending = ops.pending_purges(spark, catalog)["Account"]
    assert (pending["state"], pending["version"], pending["files"]) == ("requested", 8, ["c.parquet"])
