import csv
from datetime import UTC, date, datetime

import pytest

from sf_ingest.bulk import BulkQueryError
from sf_ingest.config import ObjectConfig
from sf_ingest.extract import PageNamer, build_soql, build_where, extract_object, write_rest_pages, year_ranges

NOW = datetime(2026, 10, 26, 17, 30, tzinfo=UTC)


def test_incremental_where_subtracts_one_hour_overlap_and_bounds_the_window():
    obj = ObjectConfig(name="Account")
    where = build_where(obj, datetime(2026, 10, 25, 17, 0, tzinfo=UTC), NOW, full=False)
    assert where == " WHERE SystemModstamp > 2026-10-25T16:00:00Z AND SystemModstamp <= 2026-10-26T17:30:00Z"


def test_first_run_has_no_lower_bound():
    assert build_where(ObjectConfig(name="Account"), None, NOW, full=False) == (
        " WHERE SystemModstamp <= 2026-10-26T17:30:00Z"
    )


def test_history_objects_use_created_date():
    obj = ObjectConfig(name="AccountHistory", watermark="CreatedDate")
    where = build_where(obj, datetime(2026, 10, 25, tzinfo=UTC), NOW, full=False)
    assert where.startswith(" WHERE CreatedDate > 2026-10-24T23:00:00Z")


def test_full_load_and_full_reload_drop_the_watermark():
    wm = datetime(2026, 10, 25, tzinfo=UTC)
    assert build_where(ObjectConfig(name="User", load_type="full"), wm, NOW, full=False) == ""
    assert build_where(ObjectConfig(name="Account"), wm, NOW, full=True) == ""


def test_soql_filter_applies_to_every_query_shape():
    obj = ObjectConfig(name="Contract", soql_filter="Id != '8002r000000vRMSAA2'")
    wm = datetime(2026, 10, 25, tzinfo=UTC)
    assert build_where(obj, wm, NOW, full=False).startswith(" WHERE (Id != '8002r000000vRMSAA2') AND SystemModstamp >")
    assert build_where(obj, wm, NOW, full=True) == " WHERE (Id != '8002r000000vRMSAA2')"
    ranged = build_where(obj, None, NOW, True, (date(2024, 1, 1), None))
    assert ranged == " WHERE (Id != '8002r000000vRMSAA2') AND CreatedDate >= 2024-01-01T00:00:00Z"


def test_split_full_reload_ranges_cover_everything():
    ranges = year_ranges(date(2024, 1, 1), date(2026, 3, 1))
    assert ranges[0] == (None, date(2024, 1, 1))
    assert ranges[-1] == (date(2026, 1, 1), None)
    assert len(ranges) == 4
    where = build_where(ObjectConfig(name="Account"), None, NOW, True, ranges[1])
    assert where == " WHERE CreatedDate >= 2024-01-01T00:00:00Z AND CreatedDate < 2025-01-01T00:00:00Z"


def test_soql_selects_every_allowed_field_without_filters():
    assert build_soql(ObjectConfig(name="Contact"), ["Id", "Name"], "") == "SELECT Id, Name FROM Contact"


def test_rest_pages_match_bulk_csv(tmp_path):
    records = [
        {
            "attributes": {},
            "Id": "01J1",
            "IsDefault": True,
            "SortOrder": 1,
            "SystemModstamp": "2026-10-25T03:04:05.000+0000",
            "ApiName": None,
        },
        {
            "attributes": {},
            "Id": "01J2",
            "IsDefault": False,
            "SortOrder": 2,
            "SystemModstamp": "2026-10-25T03:04:06.000+0000",
            "ApiName": 'Say "hi", ok',
        },
        {
            "attributes": {},
            "Id": "01J3",
            "IsDefault": False,
            "SortOrder": 3,
            "SystemModstamp": "2026-10-25T03:04:07.000+0000",
            "ApiName": "x",
        },
    ]
    fields = ["Id", "IsDefault", "SortOrder", "SystemModstamp", "ApiName"]
    pages, total = write_rest_pages(iter(records), fields, 2, PageNamer(tmp_path))
    assert total == 3
    assert [p.name for p in pages] == ["page_1.csv", "page_2.csv"]
    rows = list(csv.reader(pages[0].open()))
    assert rows[0] == fields
    assert rows[1] == ["01J1", "true", "1", "2026-10-25T03:04:05.000Z", ""]
    assert rows[2][4] == 'Say "hi", ok'


class FakeRestSession:
    api_version = "67.0"

    def __init__(self, records):
        self.queries = []
        records_ = records

        class SF:
            def query_all_iter(_, soql, include_deleted):
                self.queries.append((soql, include_deleted))
                return iter(records_)

            def query(_, soql, include_deleted):
                return {"totalSize": len(records_)}

        self.sf = SF()


def test_objects_the_bulk_api_rejects_fall_back_to_rest(tmp_path, monkeypatch):
    """UAT case: Bulk API 2.0 rejects some objects (ContractStatus, TaskStatus, maybe feeds) when the job is created."""

    def reject(self, soql, page_size, namer):
        namer().write_text("partial")  # whatever might have landed is discarded
        raise BulkQueryError(
            'Bulk API POST  returned 400: [{"errorCode":"INVALIDENTITY",'
            '"message":"Entity \'AccountFeed\' is not supported by the Bulk API."}]'
        )

    monkeypatch.setattr("sf_ingest.extract.BulkClient.query_to_files", reject)
    session = FakeRestSession([{"attributes": {}, "Id": "0D5A", "SystemModstamp": "2026-10-25T03:04:05.000+0000"}])
    out = tmp_path / "AccountFeed" / "r1"
    outcome = extract_object(
        session,
        ObjectConfig(name="AccountFeed"),
        ["Id", "SystemModstamp"],
        out,
        None,
        False,
        100,
        date(2020, 1, 1),
        NOW,
    )
    assert (outcome.api, outcome.records, outcome.pages) == ("rest", 1, 1)
    assert session.queries[0][1] is True  # queryAll: soft-deleted rows included
    assert [p.name for p in out.iterdir()] == ["page_1.csv"]
    assert list(csv.reader((out / "page_1.csv").open()))[1] == ["0D5A", "2026-10-25T03:04:05.000Z"]


def test_other_bulk_errors_are_not_retried_through_rest(tmp_path, monkeypatch):
    def fail(self, soql, page_size, namer):
        raise BulkQueryError("Bulk job 750Z Failed: INVALID_DATE_FORMAT: Invalid DateTime")

    monkeypatch.setattr("sf_ingest.extract.BulkClient.query_to_files", fail)
    session = FakeRestSession([])
    with pytest.raises(BulkQueryError, match="INVALID_DATE_FORMAT"):
        extract_object(
            session, ObjectConfig(name="Contract"), ["Id"], tmp_path / "c", None, False, 100, date(2020, 1, 1), NOW
        )
    assert session.queries == []
