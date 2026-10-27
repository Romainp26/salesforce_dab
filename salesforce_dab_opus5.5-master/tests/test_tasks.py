import os
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from simple_salesforce.exceptions import SalesforceResourceNotFound

from sf_ingest import ops, tasks
from sf_ingest.bulk import BulkQueryError
from sf_ingest.config import Approval, ConfigError, Settings, load_entity_env
from sf_ingest.extract import ExtractOutcome
from sf_ingest.metadata import FieldInfo
from sf_ingest.scd2 import scd2_table

from .conftest import ACCOUNT_FIELDS, CATALOG, CONFIG_DIR, account_config, account_row, write_page
from .conftest import ACCOUNT_HEADER as HEADER
from .test_encrypt import reference_ciphertext

pytestmark = pytest.mark.spark


class FakeContext(tasks.Context):
    """Context for local runs: local catalog, test cipher, no secrets or Salesforce."""

    def __init__(self, spark, spec, objects, run_id="r1", env="uat", reconcile_days=0, approvals=()):
        # The hard-delete check (B-25) is off unless a test turns it on: its first night depends on the date.
        target = replace(
            load_entity_env(CONFIG_DIR, "roojai", env),
            catalog=CATALOG,
            archival_user_ids=("005ARCHIVE000001AAA",),
            reconcile_days=reconcile_days,
        )
        super().__init__(
            Settings(target=target, objects=tuple(objects), approvals=approvals), spark, run_id, CONFIG_DIR
        )
        self._spec = spec

    @property
    def spec(self):
        return self._spec

    @property
    def legacy_spec(self):
        return getattr(self, "_legacy", self._spec)

    def session(self):
        return None


@pytest.fixture
def root(tmp_path, monkeypatch):
    monkeypatch.setattr(tasks, "landing_root", lambda catalog: tmp_path / "landing")
    return tmp_path / "landing"


def plan(spark, run_id, objects=("Account",), fields=ACCOUNT_FIELDS):
    ops.write_run_plan(spark, CATALOG, run_id, {o: [f.to_dict() for f in fields] for o in objects})


class FakeSalesforce:
    """Stands in for extract_object: lands the rows given per object, or raises the given error."""

    def __init__(self, monkeypatch, pages: dict):
        self.pages = pages
        self.calls = []
        self.fields = {}
        monkeypatch.setattr(tasks, "extract_object", self)

    def __call__(self, session, obj, fields, out_dir, watermark, full, *args):
        self.calls.append(obj.name)
        self.fields[obj.name] = fields
        rows = self.pages[obj.name]
        if isinstance(rows, Exception):
            raise rows
        header = rows[0] if rows and isinstance(rows[0], tuple) else HEADER
        data = [r for r in rows if not isinstance(r, tuple)]
        if data:
            write_page(out_dir, 1, list(header), data)
        return ExtractOutcome(job_ids=["750X"], pages=1 if data else 0, records=len(data), sf_count=len(data))


def run_log(spark, run_id):
    return {
        r.object: r
        for r in spark.sql(f"SELECT * FROM {ops.ops_table(CATALOG, 'run_log')} WHERE run_id = '{run_id}'").collect()
    }


def test_ingest_merges_advances_the_watermark_and_deletes_landing_files(spark, catalog, spec, root, monkeypatch):
    ctx = FakeContext(spark, spec, [account_config()])
    plan(spark, "r1")
    FakeSalesforce(monkeypatch, {"Account": [account_row("001A", modstamp="2024-05-02T02:00:00.000Z")]})
    tasks.ingest(ctx, set(), set(), workers=2)

    assert ops.get_watermark(spark, catalog, "Account") == datetime(2024, 5, 2, 2, 0, tzinfo=UTC)
    assert not (root / "Account" / "r1").exists()
    log = run_log(spark, "r1")["Account"]
    assert (log.status, log.last_stage, log.rows_extracted, log.versions_written) == ("succeeded", "ingest", 1, 1)

    # A full reload landing older records keeps the watermark where it was.
    ctx2 = FakeContext(spark, spec, [account_config()], run_id="r2")
    plan(spark, "r2")
    FakeSalesforce(monkeypatch, {"Account": [account_row("001A", modstamp="2024-05-01T02:00:00.000Z")]})
    tasks.ingest(ctx2, set(), {"Account"})
    assert ops.get_watermark(spark, catalog, "Account") == datetime(2024, 5, 2, 2, 0, tzinfo=UTC)
    log = run_log(spark, "r2")["Account"]
    assert (log.status, log.versions_written, log.watermark_after) == ("succeeded", 0, datetime(2024, 5, 2, 2, 0))


def test_no_changes_writes_nothing(spark, catalog, spec, root, monkeypatch):
    ctx = FakeContext(spark, spec, [account_config()])
    plan(spark, "r1")
    FakeSalesforce(monkeypatch, {"Account": []})
    tasks.ingest(ctx, set(), set())
    assert not spark.catalog.tableExists(f"{CATALOG}.bronze_salesforce_scd2.account")
    assert run_log(spark, "r1")["Account"].status == "succeeded"


def test_a_failed_object_never_loads_and_never_stops_the_others(spark, catalog, spec, root, monkeypatch):
    """UAT case: Contract and Payments__c fail Salesforce-side with INVALID_DATE_FORMAT in the UAT org."""
    contract = account_config(name="Contract", encrypted_fields=(), conditional_encryption=None)
    ctx = FakeContext(spark, spec, [account_config(), contract])
    plan(spark, "r1", ("Account", "Contract"))
    error = BulkQueryError("Bulk job 750Z Failed: INVALID_DATE_FORMAT: Invalid DateTime")
    sf = FakeSalesforce(monkeypatch, {"Account": [account_row("001A")], "Contract": error})

    with pytest.raises(RuntimeError, match=r"1 of 2 objects failed: \['Contract'\]"):
        tasks.ingest(ctx, set(), set())
    log = run_log(spark, "r1")
    assert (log["Contract"].status, log["Contract"].last_stage) == ("failed", "extract")
    assert "INVALID_DATE_FORMAT" in log["Contract"].error_message
    assert ops.get_watermark(spark, catalog, "Contract") is None
    assert not spark.catalog.tableExists(f"{CATALOG}.bronze_salesforce_scd2.contract")
    assert log["Account"].status == "succeeded"
    assert list(root.rglob("*.csv")) == []

    # Repairing the run skips the object that finished and retries the failed one.
    sf.calls.clear()
    sf.pages["Contract"] = [account_row("800A")]
    tasks.ingest(ctx, set(), set())
    assert sf.calls == ["Contract"]
    assert run_log(spark, "r1")["Contract"].status == "succeeded"


def test_paused_objects_are_skipped_unless_requested(spark, catalog, spec, root, monkeypatch):
    paused = account_config(paused=("uat",))
    ctx = FakeContext(spark, spec, [paused])
    plan(spark, "r1")
    sf = FakeSalesforce(monkeypatch, {"Account": [account_row("001A")]})
    tasks.ingest(ctx, set(), set())
    assert sf.calls == []
    assert (run_log(spark, "r1")["Account"].status, run_log(spark, "r1")["Account"].error_message) == (
        "skipped",
        "paused in uat",
    )

    ctx.run_id = "r2"
    plan(spark, "r2")
    tasks.ingest(ctx, {"Account"}, set())
    assert sf.calls == ["Account"]


def test_longest_first_puts_unknown_objects_first(spark, catalog, spec):
    names = ["Small", "Big", "New", "Medium"]
    ctx = FakeContext(spark, spec, [account_config(name=n) for n in names])
    now = datetime.now(UTC)
    for name, seconds in (("Small", 5), ("Big", 600), ("Medium", 60)):
        events = [("extract", now), ("ingest", now + timedelta(seconds=seconds))]
        ops.append_rows(
            spark,
            catalog,
            "run_events",
            [
                {"run_id": "old", "object": name, "stage": st, "status": "succeeded", "event_at": at}
                for st, at in events
            ],
        )
    assert [o.name for o in tasks.longest_first(ctx, list(ctx.settings.objects))] == ["New", "Big", "Medium", "Small"]


def test_cleanup_removes_this_run_and_stale_runs_only(spark, catalog, spec, root):
    ctx = FakeContext(spark, spec, [account_config()])
    for run in ("r1", "stale-run", "running-backfill"):
        write_page(root / "Account" / run, 1, HEADER, [account_row("001A")])
    old = time.time() - 13 * 3600
    os.utime(root / "Account" / "stale-run", (old, old))

    tasks.cleanup(ctx)
    assert sorted(p.parent.name for p in root.rglob("*.csv")) == ["running-backfill"]
    dq_row = spark.table(ops.ops_table(catalog, "dq_results")).first()
    assert (dq_row.check_name, dq_row.passed) == ("landing_volume_empty", True)


def test_preflight_applies_new_field_policies_and_approvals(spark, catalog, spec, monkeypatch):
    auto = account_config()
    approval = replace(
        account_config(name="Contact", encrypted_fields=(), conditional_encryption=None), new_fields="require-approval"
    )
    ctx = FakeContext(spark, spec, [auto, approval])
    described = {
        "Account": [*ACCOUNT_FIELDS, FieldInfo("Brand_New__c", "string")],
        "Contact": [*ACCOUNT_FIELDS, FieldInfo("Needs_Review__c", "string")],
    }
    monkeypatch.setattr(ops, "ensure_uc_objects", lambda *a: [])
    monkeypatch.setattr(ctx, "salesforce", lambda: (type("S", (), {"sf": None})(), None, None))
    monkeypatch.setattr(tasks, "describe_all", lambda sf, names: (described, {}))
    monkeypatch.setattr(tasks, "seed_allowlist", lambda c: None)
    rows = [
        {"object": o, "field": f.name, "allowed": 1, "source": "seed"}
        for o in ("Account", "Contact")
        for f in ACCOUNT_FIELDS
    ]
    ops.append_rows(spark, catalog, "field_allowlist", rows)

    tasks.preflight(ctx)
    plans = {o: [f["name"] for f in ops.read_run_plan(spark, catalog, "r1", o)] for o in ("Account", "Contact")}
    assert "Brand_New__c" in plans["Account"]
    assert "Needs_Review__c" not in plans["Contact"]
    pending = spark.table(ops.ops_table(catalog, "field_approval")).collect()
    assert [(r.object, r.field, r.status) for r in pending] == [("Contact", "Needs_Review__c", "pending")]

    # Approving the field in config/approvals (B-28) puts it in the next night's query and answers the pending row;
    # a second preflight doesn't duplicate approvals.
    decision = Approval("Contact", "Needs_Review__c", "approved", False, "owner", "2026-11-02")
    ctx.settings = replace(ctx.settings, approvals=(decision,))
    ctx.run_id = "r2"
    tasks.preflight(ctx)
    assert "Needs_Review__c" in [f["name"] for f in ops.read_run_plan(spark, catalog, "r2", "Contact")]
    rows = spark.table(ops.ops_table(catalog, "field_approval")).collect()
    assert [(r.status, r.decided_by) for r in rows] == [("approved", "owner")]
    ctx.run_id = "r3"
    tasks.preflight(ctx)
    assert spark.table(ops.ops_table(catalog, "field_approval")).count() == 1
    allow = spark.table(ops.ops_table(catalog, "field_allowlist")).where("field = 'Needs_Review__c'").collect()
    assert [(r.allowed, r.source) for r in allow] == [(1, "approval")]
    dq_checks = {r.check_name for r in spark.table(ops.ops_table(catalog, "dq_results")).collect()}
    assert {"new_field_auto_added", "certificate_expiry"} <= dq_checks


def test_one_object_that_cant_be_planned_never_stops_the_others(spark, catalog, spec, root, monkeypatch):
    """UAT case: an object missing from the sandbox, a describe error and an unusable plan, beside a good object."""
    plain = dict(encrypted_fields=(), conditional_encryption=None)
    names = ("Gone__c", "Broken__c", "NoWatermark__c")
    paused_gone = account_config(name="PausedGone__c", paused=("uat",), **plain)
    ctx = FakeContext(spark, spec, [account_config(), *(account_config(name=n, **plain) for n in names), paused_gone])
    missing = SalesforceResourceNotFound("https://x/sobjects/Gone__c/describe", 404, "Gone__c", [])
    described = {"Account": ACCOUNT_FIELDS, "NoWatermark__c": ACCOUNT_FIELDS}
    errors = {"Gone__c": missing, "PausedGone__c": missing, "Broken__c": RuntimeError("503 Service Unavailable")}
    monkeypatch.setattr(ops, "ensure_uc_objects", lambda *a: [])
    monkeypatch.setattr(ctx, "salesforce", lambda: (type("S", (), {"sf": None})(), None, None))
    monkeypatch.setattr(tasks, "describe_all", lambda sf, names: (described, errors))
    monkeypatch.setattr(tasks, "seed_allowlist", lambda c: None)
    allow = [{"object": "Account", "field": f.name, "allowed": 1, "source": "seed"} for f in ACCOUNT_FIELDS]
    allow += [
        {"object": "NoWatermark__c", "field": f.name, "allowed": int(f.name != "SystemModstamp"), "source": "seed"}
        for f in ACCOUNT_FIELDS
    ]
    ops.append_rows(spark, catalog, "field_allowlist", allow)
    ops.append_rows(spark, catalog, "object_schema", [{"object": "Broken__c", "field": "Id", "sf_type": "id"}])

    tasks.preflight(ctx)
    assert ops.planned_objects(spark, catalog, "r1") == {"Account"}
    # The objects not described tonight keep their schema snapshot for the next removed/retyped comparison.
    assert {r.object for r in spark.table(ops.ops_table(catalog, "object_schema")).collect()} == {
        "Account",
        "Broken__c",
    }
    # A missing object paused in this environment is acknowledged: no warning.
    warnings = spark.table(ops.ops_table(catalog, "dq_results")).where("check_name = 'object_planned'").collect()
    assert sorted(r.object for r in warnings) == sorted(names)

    FakeSalesforce(monkeypatch, {"Account": [account_row("001A")]})
    with pytest.raises(RuntimeError, match=r"2 of 3 objects failed: \['Broken__c', 'NoWatermark__c'\]"):
        tasks.ingest(ctx, set(), set())
    log = run_log(spark, "r1")
    assert (log["Account"].status, log["Account"].last_stage) == ("succeeded", "ingest")
    assert (log["Gone__c"].status, log["Gone__c"].last_stage, log["Gone__c"].error_message) == (
        "skipped",
        "preflight",
        "not in the uat org",
    )
    assert (log["Broken__c"].status, log["Broken__c"].last_stage) == ("failed", "preflight")
    assert "503" in log["Broken__c"].error_message
    assert "SystemModstamp must be described and allowlisted" in log["NoWatermark__c"].error_message
    assert (log["PausedGone__c"].status, log["PausedGone__c"].error_message) == ("skipped", "paused in uat")


def test_backfill_fills_current_versions_only(spark, catalog, spec, root, monkeypatch):
    ctx = FakeContext(spark, spec, [account_config()])
    plan(spark, "r1")
    FakeSalesforce(
        monkeypatch,
        {
            "Account": [
                account_row("001A", modstamp="2024-05-01T02:00:00.000Z"),
                account_row("001A", name="Acme Renamed", modstamp="2024-05-02T02:00:00.000Z"),
            ]
        },
    )
    tasks.ingest(ctx, set(), set())
    FakeSalesforce(
        monkeypatch, {"Account": [account_row("001A", name="Acme Two", modstamp="2024-05-03T02:00:00.000Z")]}
    )
    ctx.run_id = "r2"
    plan(spark, "r2")
    tasks.ingest(ctx, set(), set())

    # Region__c was approved after both versions were written; Name is conditionally encrypted.
    fields = [*ACCOUNT_FIELDS, FieldInfo("Region__c", "picklist")]
    ctx.run_id = "b1"
    plan(spark, "b1", fields=fields)
    sf = FakeSalesforce(
        monkeypatch,
        {
            "Account": [
                ("Id", "Region__c", "Name", "Type"),
                ["001A", "North", "Acme Three", "Customer"],
                ["001Z", "South", "Zed", "Customer"],
            ]
        },
    )
    tasks.backfill(ctx, "Account.Region__c,Account.Name")

    assert sf.fields["Account"] == ["Id", "Region__c", "Name", "Type"]  # Id and Type: the Name rule reads them
    rows = spark.sql(f"SELECT * FROM {scd2_table(catalog, account_config())} ORDER BY _valid_from").collect()
    assert [(r.Region__c, r._is_current) for r in rows] == [(None, False), ("North", True)]
    assert rows[1].Name == reference_ciphertext("Acme Three")
    assert ops.get_watermark(spark, catalog, "Account") == datetime(2024, 5, 3, 2, 0, tzinfo=UTC)
    assert list(root.rglob("*.csv")) == []
    assert run_log(spark, "b1")["Account"].versions_written == 1


def test_backfill_rejects_history_ids_and_watermarks(spark, spec):
    ctx = FakeContext(spark, spec, [account_config()])
    for bad in ("Account.Id", "Account.SystemModstamp", "Account", ""):
        with pytest.raises(SystemExit):
            tasks.parse_backfill(ctx.settings, bad)
    with pytest.raises(ConfigError):
        tasks.parse_backfill(ctx.settings, "Nope.X")
    assert tasks.parse_backfill(ctx.settings, "Account.A__c, Account.a__c,Account.B__c") == {
        "Account": ["A__c", "B__c"]
    }


def test_a_missing_object_fails_in_prod(spark, catalog, spec, root, monkeypatch):
    """In PROD a describe 404 is also a lost permission: failed, not skipped (B-23). Pausing acknowledges it."""
    plain = dict(encrypted_fields=(), conditional_encryption=None)
    objects = [
        account_config(),
        account_config(name="Gone__c", **plain),
        account_config(name="Paused__c", paused=("prod",), **plain),
    ]
    ctx = FakeContext(spark, spec, objects, env="prod")
    missing = SalesforceResourceNotFound("https://x/sobjects/Gone__c/describe", 404, "Gone__c", [])
    monkeypatch.setattr(ops, "ensure_uc_objects", lambda *a: [])
    monkeypatch.setattr(ctx, "salesforce", lambda: (type("S", (), {"sf": None})(), None, None))
    monkeypatch.setattr(
        tasks,
        "describe_all",
        lambda sf, names: ({"Account": ACCOUNT_FIELDS}, {"Gone__c": missing, "Paused__c": missing}),
    )
    monkeypatch.setattr(tasks, "seed_allowlist", lambda c: None)
    ops.append_rows(
        spark,
        catalog,
        "field_allowlist",
        [{"object": "Account", "field": f.name, "allowed": 1} for f in ACCOUNT_FIELDS],
    )
    tasks.preflight(ctx)
    outcomes = ops.preflight_outcomes(spark, catalog, "r1")
    assert outcomes["Gone__c"][0] == "failed" and "can't read it" in outcomes["Gone__c"][1]
    assert outcomes["Paused__c"][0] == "skipped"


def test_a_first_load_never_runs_before_the_seed(spark, catalog, spec, root, monkeypatch):
    """B-34: a backup table that wasn't seeded yet blocks the object's first load from Salesforce."""
    ctx = FakeContext(spark, spec, [account_config()])
    plan(spark, "r1")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`backup_salesforce`")
    spark.createDataFrame([("001A",)], "Id STRING").write.saveAsTable(f"{CATALOG}.backup_salesforce.account")
    sf = FakeSalesforce(monkeypatch, {"Account": [account_row("001A")]})
    with pytest.raises(RuntimeError, match="1 of 1 objects failed"):
        tasks.ingest(ctx, set(), set())
    assert sf.calls == []
    assert "hasn't been seeded" in run_log(spark, "r1")["Account"].error_message

    # A reset object (runbook §5.9: table dropped, backup kept) was ingested before: it reloads from Salesforce.
    ops.append_event(spark, catalog, "r0", "Account", "ingest", "succeeded")
    tasks.check_unseeded(ctx, account_config())


def test_incremental_objects_are_checked_for_hard_deletes_when_due(spark, catalog, spec, root, monkeypatch):
    """B-25, with reconcile_days = 1: every night."""
    ctx = FakeContext(spark, spec, [account_config()], reconcile_days=1)
    plan(spark, "r1")
    rows = [account_row(f"001{i:02d}") for i in range(30)]
    sf = FakeSalesforce(monkeypatch, {"Account": rows})
    tasks.ingest(ctx, set(), set())
    assert sf.calls == ["Account", "Account"]  # the changes, then the Id-only check
    assert sf.fields["Account"] == ["Id"]

    # The next night one record is gone from Salesforce, recycle bin included.
    ctx.run_id = "r2"
    plan(spark, "r2")
    FakeSalesforce(monkeypatch, {"Account": rows[1:]})
    monkeypatch.setattr(tasks, "reconcile_due", lambda *a: True)
    tasks.ingest(ctx, set(), set())
    events = spark.table(ops.ops_table(catalog, "run_events")).where("run_id = 'r2' AND stage = 'reconcile'").first()
    assert (events.status, events.hard_deletes) == ("succeeded", 1)
    flagged = spark.sql(
        f"SELECT Id FROM {scd2_table(catalog, account_config())} WHERE _is_current AND _is_hard_deleted"
    )
    assert [r.Id for r in flagged.collect()] == ["00100"]


def test_reconcile_is_spread_and_repeated(spark, spec):
    ctx = FakeContext(spark, spec, [], reconcile_days=7)
    now = datetime(2026, 11, 1, 18, 0, tzinfo=UTC)
    obj = account_config()
    assert tasks.reconcile_due(ctx, obj, now - timedelta(days=7, minutes=-20), now)  # a night later than 7 days
    assert not tasks.reconcile_due(ctx, obj, now - timedelta(days=5), now)
    assert not tasks.reconcile_due(ctx, account_config(load_type="full"), None, now)
    first_nights = sum(tasks.reconcile_due(ctx, account_config(name=f"O{i}"), None, now) for i in range(70))
    assert 0 < first_nights < 70  # spread over the week, not all on one night


def test_purge_removes_the_plaintext_history_of_a_re_encrypted_table(spark, catalog, spec, root, monkeypatch):
    """B-21: after a PII column is encrypted in place, its plaintext versions become unreadable, then the log goes."""
    plain = account_config(encrypted_fields=(), conditional_encryption=None)
    ctx = FakeContext(spark, spec, [plain])
    plan(spark, "r1")
    FakeSalesforce(monkeypatch, {"Account": [account_row("001A"), account_row("001B", phone="")]})
    tasks.ingest(ctx, set(), set())
    table = scd2_table(catalog, plain)

    flagged = account_config(conditional_encryption=None)
    ctx = FakeContext(spark, spec, [flagged], run_id="r2")
    plan(spark, "r2")
    FakeSalesforce(monkeypatch, {"Account": []})  # a quiet night still applies the new PII config (B-21)
    tasks.ingest(ctx, set(), set())
    assert {r.Phone__c for r in spark.table(table).collect()} == {reference_ciphertext("0812345678"), None}
    before = ops.pending_purges(spark, catalog)["Account"]["version"]
    old = f"SELECT count_if(Phone__c = '0812345678') FROM {table} VERSION AS OF {before}"
    assert spark.sql(old).first()[0] == 1

    # Night 1: REORG, and the files still holding plaintext are recorded. The file retention is 12 hours in jobs.
    monkeypatch.setattr(tasks, "PURGE_FILE_RETENTION", timedelta(0))
    tasks.purge(ctx)
    pending = ops.pending_purges(spark, catalog)["Account"]
    assert pending["state"] == "reorged" and pending["files"]
    location = spark.sql(f"DESCRIBE DETAIL {table}").first().location.replace("file:", "")
    assert all(Path(location, f).exists() for f in pending["files"])

    # Night 2: VACUUM deletes them.
    ctx.run_id = "r2b"
    tasks.purge(ctx)
    assert not any(Path(location, f).exists() for f in pending["files"])
    result = spark.table(ops.ops_table(catalog, "dq_results")).where("check_name = 'pii_history_purged'").first()
    assert result.passed
    assert ops.pending_purges(spark, catalog)["Account"]["state"] == "files_purged"
    assert not any(b"0812345678" in f.read_bytes() for f in Path(location).glob("*.parquet"))

    # Two nights later the log holding the old files' statistics has expired.
    aged = time.time() - 3 * 86400
    for f in Path(location, "_delta_log").iterdir():
        os.utime(f, (aged, aged))
    ctx.run_id = "r3"
    tasks.purge(ctx)
    assert "Account" not in ops.pending_purges(spark, catalog)
    assert not any(b"0812345678" in f.read_bytes() for f in Path(location, "_delta_log").glob("*.json"))
    props = {r.key for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    assert "delta.deletedFileRetentionDuration" not in props


def test_the_purge_waits_for_the_file_retention_before_vacuuming(spark, catalog, spec, root, monkeypatch):
    plain = account_config(encrypted_fields=(), conditional_encryption=None)
    ctx = FakeContext(spark, spec, [plain])
    plan(spark, "r1")
    FakeSalesforce(monkeypatch, {"Account": [account_row("001A")]})
    tasks.ingest(ctx, set(), set())
    ctx = FakeContext(spark, spec, [account_config(conditional_encryption=None)], run_id="r2")
    plan(spark, "r2")
    FakeSalesforce(monkeypatch, {"Account": []})
    tasks.ingest(ctx, set(), set())
    tasks.purge(ctx)
    files = ops.pending_purges(spark, catalog)["Account"]["files"]
    ctx.run_id = "r3"
    tasks.purge(ctx)  # less than 12 hours later: a concurrent writer's uncommitted files must survive
    assert ops.pending_purges(spark, catalog)["Account"]["state"] == "reorged"
    location = spark.sql(f"DESCRIBE DETAIL {scd2_table(catalog, plain)}").first().location.replace("file:", "")
    assert all(Path(location, f).exists() for f in files)
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {scd2_table(catalog, plain)}").collect()}
    assert props["delta.deletedFileRetentionDuration"] == "interval 12 hours"


def test_a_paused_object_still_gets_its_new_pii_config(spark, catalog, spec, root, monkeypatch):
    """B-21: a newly listed PII column of an object paused tonight is encrypted all the same."""
    plain = account_config(encrypted_fields=(), conditional_encryption=None)
    plan(spark, "r1")
    FakeSalesforce(monkeypatch, {"Account": [account_row("001A")]})
    tasks.ingest(FakeContext(spark, spec, [plain]), set(), set())

    paused = account_config(conditional_encryption=None, paused=("uat",))
    sf = FakeSalesforce(monkeypatch, {"Account": [account_row("001B")]})
    tasks.ingest(FakeContext(spark, spec, [paused], run_id="r2"), set(), set())  # no plan: the last snapshot
    assert sf.calls == []
    table = scd2_table(catalog, plain)
    assert {r.Phone__c for r in spark.table(table).collect()} == {reference_ciphertext("0812345678")}
    assert run_log(spark, "r2")["Account"].status == "skipped"


def test_tables_of_unconfigured_objects_are_reported_but_temp_views_are_not(spark, catalog, spec):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`bronze_salesforce_scd2`")
    spark.sql(f"CREATE TABLE `{CATALOG}`.`bronze_salesforce_scd2`.`gone__c` (Id STRING) USING DELTA")
    spark.range(1).createOrReplaceTempView("plan_scratch")
    found = tasks.orphaned_objects(FakeContext(spark, spec, [account_config()]))
    assert [(r["check_name"], r["object"]) for r in found] == [("object_orphaned", "gone__c")]
