import json

import pytest
import requests
import responses

from sf_ingest.bulk import BulkClient, BulkQueryError

BASE = "https://acme.my.salesforce.com/services/data/v67.0/jobs/query"


class FakeSession:
    instance_url = "https://acme.my.salesforce.com"

    def __init__(self):
        self.access_token = "token-1"
        self.refreshes = 0

    def refresh(self):
        self.refreshes += 1
        self.access_token = f"token-{self.refreshes + 1}"


def client(session=None):
    return BulkClient(session or FakeSession(), "67.0", sleep=lambda s: None)


def namer(tmp_path):
    counter = iter(range(1, 100))
    return lambda: tmp_path / f"page_{next(counter)}.csv"


@responses.activate
def test_query_pages_follow_the_locator_and_land_unparsed(tmp_path):
    responses.post(BASE, json={"id": "750J"})
    responses.get(f"{BASE}/750J", json={"state": "InProgress"})
    responses.get(f"{BASE}/750J", json={"state": "JobComplete"})
    page1 = b'"Id","Name"\n"001A","Acme, Inc"\n"001B","Beta"\n'
    page2 = b'"Id","Name"\n"001C","Gamma"\n'
    responses.get(
        f"{BASE}/750J/results",
        body=page1,
        headers={"Sforce-NumberOfRecords": "2", "Sforce-Locator": "LOC2"},
        match=[responses.matchers.query_param_matcher({"maxRecords": "2"})],
    )
    responses.get(
        f"{BASE}/750J/results",
        body=page2,
        headers={"Sforce-NumberOfRecords": "1", "Sforce-Locator": "null"},
        match=[responses.matchers.query_param_matcher({"maxRecords": "2", "locator": "LOC2"})],
    )

    result = client().query_to_files("SELECT Id, Name FROM Account", 2, namer(tmp_path))

    assert result.job_id == "750J"
    assert result.records == 3
    assert [p.name for p in result.pages] == ["page_1.csv", "page_2.csv"]
    assert result.pages[0].read_bytes() == page1
    assert result.pages[1].read_bytes() == page2
    body = json.loads(responses.calls[0].request.body)
    assert body["operation"] == "queryAll"
    assert body["contentType"] == "CSV"
    assert not list(tmp_path.glob("*.tmp"))


@responses.activate
def test_empty_result_writes_no_page(tmp_path):
    responses.post(BASE, json={"id": "750E"})
    responses.get(f"{BASE}/750E", json={"state": "JobComplete"})
    responses.get(f"{BASE}/750E/results", body=b"", headers={"Sforce-NumberOfRecords": "0", "Sforce-Locator": "null"})
    result = client().query_to_files("SELECT Id FROM Account", 10, namer(tmp_path))
    assert result.records == 0
    assert result.pages == []


@responses.activate
def test_reauthenticates_once_on_401():
    session = FakeSession()
    responses.post(BASE, status=401)
    responses.post(BASE, json={"id": "750R"})
    assert client(session).create_job("SELECT Id FROM Account") == "750R"
    assert session.refreshes == 1
    assert responses.calls[1].request.headers["Authorization"] == "Bearer token-2"


@responses.activate
def test_retries_throttling_and_server_errors(tmp_path):
    responses.post(BASE, json={"id": "750T"})
    responses.get(f"{BASE}/750T", json={"state": "JobComplete"})
    responses.get(f"{BASE}/750T/results", status=503)
    responses.get(f"{BASE}/750T/results", status=429)
    responses.get(f"{BASE}/750T/results", body=b'"Id"\n"001A"\n', headers={"Sforce-NumberOfRecords": "1"})
    result = client().query_to_files("SELECT Id FROM Account", 10, namer(tmp_path))
    assert result.records == 1


@responses.activate
def test_failed_job_raises():
    responses.post(BASE, json={"id": "750F"})
    responses.get(f"{BASE}/750F", json={"state": "Failed", "errorMessage": "INVALID_FIELD"})
    with pytest.raises(BulkQueryError, match="INVALID_FIELD"):
        client().query_to_files("SELECT Nope FROM Account", 10, lambda: None)


@responses.activate
def test_client_errors_are_not_retried():
    responses.post(BASE, status=400, body="MALFORMED_QUERY")
    with pytest.raises(BulkQueryError, match="MALFORMED_QUERY"):
        client().create_job("SELEC")
    assert len(responses.calls) == 1


class DroppingStream:
    """A results response whose body stream fails after the first chunk, the first `drops` times."""

    def __init__(self, body: bytes, drops: int):
        self.body, self.drops, self.calls = body, drops, 0

    def request(self, method, url, headers=None, timeout=None, params=None, stream=False, **kwargs):
        self.calls += 1
        fail = self.calls <= self.drops
        body = self.body

        class Response:
            status_code = 200
            headers = {"Sforce-NumberOfRecords": "2", "Sforce-Locator": "null"}
            text = ""

            def iter_content(self, chunk_size):
                yield body[:5]
                if fail:
                    raise requests.exceptions.ChunkedEncodingError("Connection broken")
                yield body[5:]

            def close(self):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        return Response()


def test_a_dropped_page_download_is_fetched_again(tmp_path):
    body = b'"Id"\n"001A"\n"001B"\n'
    http = DroppingStream(body, drops=2)
    bulk = BulkClient(FakeSession(), "67.0", http=http, sleep=lambda s: None)
    result = bulk.download_pages("750D", 10, namer(tmp_path))
    assert http.calls == 3
    assert result.records == 2
    assert [p.read_bytes() for p in result.pages] == [body]
    assert not list(tmp_path.glob("*.tmp"))


def test_a_page_that_keeps_dropping_fails(tmp_path):
    bulk = BulkClient(FakeSession(), "67.0", http=DroppingStream(b"0123456789", drops=9), sleep=lambda s: None)
    with pytest.raises(requests.exceptions.ChunkedEncodingError):
        bulk.download_pages("750D", 10, namer(tmp_path))
    assert not list(tmp_path.iterdir())


@responses.activate
def test_a_job_still_running_after_the_maximum_wait_is_aborted():
    now = [0.0]

    def sleep(seconds):
        now[0] += seconds

    responses.get(f"{BASE}/750S", json={"state": "InProgress"})
    responses.patch(f"{BASE}/750S", json={"id": "750S", "state": "Aborted"})
    bulk = BulkClient(FakeSession(), "67.0", poll_seconds=60, max_wait_seconds=300, sleep=sleep, clock=lambda: now[0])

    with pytest.raises(BulkQueryError, match="750S still InProgress after 5 minutes; aborted"):
        bulk.wait_for_job("750S")
    abort = responses.calls[-1].request
    assert (abort.method, json.loads(abort.body)) == ("PATCH", {"state": "Aborted"})
    assert now[0] == 300
