"""Thin Bulk API 2.0 query client on `requests` (D-21).

Each result page is streamed to its own file exactly as Salesforce returns it, so memory stays flat
however large the object is.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

import requests

log = logging.getLogger(__name__)

RETRY_STATUSES = {429, 500, 502, 503, 504}
# A connection that drops while a page streams: the page is fetched again by its locator.
STREAM_ERRORS = (requests.exceptions.ChunkedEncodingError, requests.ConnectionError, requests.Timeout)
# Longest wait for one query job to complete. Below the ingest task's timeout (6 hours by default), so a stuck
# job fails its own object and the others finish.
MAX_JOB_WAIT_SECONDS = 3 * 3600


class BulkQueryError(RuntimeError):
    pass


def is_unsupported_entity(exc: Exception) -> bool:
    """Salesforce's answer when Bulk API 2.0 can't query an object (for example ContractStatus, TaskStatus)."""
    return isinstance(exc, BulkQueryError) and "not supported by the bulk api" in str(exc).lower()


class TokenProvider(Protocol):
    """What the client needs from an authenticated session (see auth.SalesforceSession)."""

    instance_url: str
    access_token: str

    def refresh(self) -> None: ...


@dataclass
class QueryResult:
    job_id: str
    pages: list[Path] = field(default_factory=list)
    records: int = 0


class BulkClient:
    def __init__(
        self,
        session: TokenProvider,
        api_version: str,
        http: requests.Session | None = None,
        max_attempts: int = 5,
        backoff_seconds: float = 5.0,
        poll_seconds: float = 5.0,
        max_poll_seconds: float = 60.0,
        max_wait_seconds: float = MAX_JOB_WAIT_SECONDS,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._session = session
        self._api_version = api_version
        self._http = http or requests.Session()
        self._max_attempts = max_attempts
        self._backoff = backoff_seconds
        self._poll = poll_seconds
        self._max_poll = max_poll_seconds
        self._max_wait = max_wait_seconds
        self._sleep = sleep
        self._clock = clock

    def _url(self, path: str) -> str:
        return f"{self._session.instance_url}/services/data/v{self._api_version}/jobs/query{path}"

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        """Send one request, re-authenticating once on 401 and backing off on throttling or 5xx.

        A POST creates a query job, so it is retried only when Salesforce can't have received it (a connect timeout)
        or refused it (429): a read timeout or a 5xx may already have created the job, and a retry would leave a
        duplicate one running (B-34).
        """
        reauthed = False
        attempt = 0
        idempotent = method != "POST"
        retry_statuses = RETRY_STATUSES if idempotent else {429}
        while True:
            attempt += 1
            headers = {"Authorization": f"Bearer {self._session.access_token}", **kwargs.pop("headers", {})}
            try:
                resp = self._http.request(method, self._url(path), headers=headers, timeout=300, **kwargs)
            except (requests.ConnectionError, requests.Timeout) as exc:
                if attempt >= self._max_attempts or not (idempotent or isinstance(exc, requests.ConnectTimeout)):
                    raise
                log.warning("Bulk API %s %s failed (%s); retrying", method, path, type(exc).__name__)
                self._sleep(self._backoff * 2 ** (attempt - 1))
                continue
            if resp.status_code == 401 and not reauthed:
                resp.close()
                reauthed = True
                self._session.refresh()
                continue
            if resp.status_code in retry_statuses and attempt < self._max_attempts:
                resp.close()
                log.warning("Bulk API %s %s returned %s; retrying", method, path, resp.status_code)
                self._sleep(self._backoff * 2 ** (attempt - 1))
                continue
            if resp.status_code >= 400:
                raise BulkQueryError(f"Bulk API {method} {path} returned {resp.status_code}: {resp.text[:500]}")
            return resp

    def create_job(self, soql: str) -> str:
        body = {
            # queryAll also returns records in the recycle bin, flagged IsDeleted = true.
            "operation": "queryAll",
            "query": soql,
            "contentType": "CSV",
            "columnDelimiter": "COMMA",
            "lineEnding": "LF",
        }
        return self._request("POST", "", json=body).json()["id"]

    def wait_for_job(self, job_id: str) -> dict:
        """Poll until the job completes. A job still running after `max_wait_seconds` is aborted and fails the
        object, so one stuck job can't hold the ingest task until its timeout."""
        delay = self._poll
        deadline = self._clock() + self._max_wait
        while True:
            info = self._request("GET", f"/{job_id}").json()
            state = info["state"]
            if state == "JobComplete":
                return info
            if state in ("Failed", "Aborted"):
                raise BulkQueryError(f"Bulk job {job_id} {state}: {info.get('errorMessage', '')}")
            if self._clock() >= deadline:
                try:
                    self._request("PATCH", f"/{job_id}", json={"state": "Aborted"}).close()
                except Exception as exc:  # the job times out Salesforce-side anyway; report the wait, not this
                    log.warning("Bulk job %s: abort failed (%s)", job_id, type(exc).__name__)
                raise BulkQueryError(
                    f"Bulk job {job_id} still {state} after {self._max_wait / 60:.0f} minutes; aborted"
                )
            self._sleep(delay)
            delay = min(delay * 1.5, self._max_poll)

    def download_pages(self, job_id: str, page_size: int, next_page_path: Callable[[], Path]) -> QueryResult:
        """Follow the result locator, writing each page to the path `next_page_path` returns."""
        result = QueryResult(job_id=job_id)
        locator: str | None = None
        while True:
            params = {"maxRecords": page_size}
            if locator:
                params["locator"] = locator
            records, next_locator, path = self._download_page(job_id, params, next_page_path)
            if path is not None:
                result.pages.append(path)
                result.records += records
            if not next_locator or next_locator == "null":
                return result
            locator = next_locator

    def _download_page(self, job_id: str, params: dict, next_page_path: Callable[[], Path]):
        """Stream one page to a file. A page is replayable by its locator, so a dropped stream restarts that page."""
        path: Path | None = None
        attempt = 0
        while True:
            attempt += 1
            resp = self._request("GET", f"/{job_id}/results", params=params, stream=True)
            records = int(resp.headers.get("Sforce-NumberOfRecords", "0"))
            next_locator = resp.headers.get("Sforce-Locator")
            if not records:
                resp.close()
                return 0, next_locator, None
            path = path or next_page_path()
            tmp = path.with_suffix(".tmp")
            try:
                with resp, tmp.open("wb") as out:
                    for chunk in resp.iter_content(chunk_size=1 << 20):
                        out.write(chunk)
            except STREAM_ERRORS as exc:
                tmp.unlink(missing_ok=True)
                if attempt >= self._max_attempts:
                    raise
                log.warning(
                    "Bulk job %s: page download dropped (%s), attempt %d/%d; fetching it again",
                    job_id,
                    type(exc).__name__,
                    attempt,
                    self._max_attempts,
                )
                self._sleep(self._backoff * 2 ** (attempt - 1))
                continue
            os.replace(tmp, path)
            return records, next_locator, path

    def query_to_files(self, soql: str, page_size: int, next_page_path: Callable[[], Path]) -> QueryResult:
        job_id = self.create_job(soql)
        log.info("Bulk job %s created", job_id)
        info = self.wait_for_job(job_id)
        result = self.download_pages(job_id, page_size, next_page_path)
        # The job's own count is exact, unlike a later COUNT(): a short download must never advance the watermark
        # past the records it lost (B-35).
        expected = info.get("numberRecordsProcessed")
        if expected is not None and int(expected) != result.records:
            raise BulkQueryError(f"Bulk job {job_id} processed {expected} records but {result.records} were downloaded")
        return result
