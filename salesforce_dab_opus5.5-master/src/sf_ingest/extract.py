"""Extract one object into the landing volume: one raw CSV file per result page (D-08, D-13)."""

from __future__ import annotations

import csv
import itertools
import logging
import re
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from .auth import SalesforceSession
from .bulk import BulkClient, is_unsupported_entity
from .config import ObjectConfig

log = logging.getLogger(__name__)

OVERLAP = timedelta(hours=1)
SPLIT_WORKERS = 4


def landing_root(catalog: str) -> Path:
    return Path(f"/Volumes/{catalog}/ops_salesforce/landing")


def landing_dir(root: Path, obj: str, run_id: str) -> Path:
    return root / obj / run_id


def soql_datetime(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def year_ranges(split_from: date, today: date) -> list[tuple[date | None, date | None]]:
    """CreatedDate ranges for a split full reload; the first and last ranges are open-ended."""
    bounds = [date(y, 1, 1) for y in range(split_from.year, today.year + 1)]
    return list(itertools.pairwise([None, *bounds, None]))


def build_where(
    obj: ObjectConfig,
    watermark_utc: datetime | None,
    until_utc: datetime,
    full: bool,
    created_range: tuple[date | None, date | None] | None = None,
) -> str:
    """WHERE clause for the extract and its COUNT() check.

    Record exclusions live in views (D-17); the only record filter here is the object's `soql_filter`, for records
    Salesforce can't return at all.
    """
    conds = [f"({obj.soql_filter})"] if obj.soql_filter else []
    if obj.load_type == "incremental" and not full:
        if watermark_utc is not None:
            conds.append(f"{obj.watermark} > {soql_datetime(watermark_utc - OVERLAP)}")
        # The upper bound makes the COUNT() check comparable; later changes come with the next run.
        conds.append(f"{obj.watermark} <= {soql_datetime(until_utc)}")
    if created_range:
        start, end = created_range
        if start:
            conds.append(f"CreatedDate >= {start.isoformat()}T00:00:00Z")
        if end:
            conds.append(f"CreatedDate < {end.isoformat()}T00:00:00Z")
    return f" WHERE {' AND '.join(conds)}" if conds else ""


def build_soql(obj: ObjectConfig, fields: list[str], where: str) -> str:
    return f"SELECT {', '.join(fields)} FROM {obj.name}{where}"


class PageNamer:
    """Thread-safe page_<n>.csv names, shared by parallel range queries."""

    def __init__(self, directory: Path):
        self._dir = directory
        self._counter = itertools.count(1)
        self._lock = threading.Lock()

    def __call__(self) -> Path:
        with self._lock:
            return self._dir / f"page_{next(self._counter)}.csv"


_REST_DATETIME = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3})\+0000$")


def _rest_value(value) -> str:
    """Render a REST JSON value the way Bulk API 2.0 CSV does."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value)
    if m := _REST_DATETIME.match(text):
        return f"{m.group(1)}Z"
    return text


def write_rest_pages(records, fields: list[str], page_size: int, next_page_path) -> tuple[list[Path], int]:
    """Write REST queryAll records as Bulk-style CSV pages (for objects the Bulk API can't query)."""
    pages: list[Path] = []
    total = 0
    for batch in itertools.batched(records, page_size):
        path = next_page_path()
        with path.open("w", newline="", encoding="utf-8") as out:
            writer = csv.writer(out, quoting=csv.QUOTE_ALL, lineterminator="\n")
            writer.writerow(fields)
            for rec in batch:
                writer.writerow([_rest_value(rec.get(f)) for f in fields])
        pages.append(path)
        total += len(batch)
    return pages, total


@dataclass
class ExtractOutcome:
    job_ids: list[str] = field(default_factory=list)
    pages: int = 0
    records: int = 0
    sf_count: int | None = None
    api: str = "bulk"
    # The extract's upper bound: a version this run writes "tonight" starts here, so a change made while the run
    # loads is newer and arrives with the next one.
    started_utc: datetime | None = None


def extract_object(
    session: SalesforceSession,
    obj: ObjectConfig,
    fields: list[str],
    out_dir: Path,
    watermark_utc: datetime | None,
    full: bool,
    page_size: int,
    split_from: date,
    now_utc: datetime,
) -> ExtractOutcome:
    # A repaired run re-extracts into a clean folder, so stale pages never mix with new ones.
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True)
    namer = PageNamer(out_dir)
    outcome = ExtractOutcome(started_utc=now_utc)
    where = build_where(obj, watermark_utc, now_utc, full)

    def rest():
        soql = build_soql(obj, fields, where)
        pages, records = write_rest_pages(
            session.sf.query_all_iter(soql, include_deleted=True), fields, page_size, namer
        )
        outcome.pages, outcome.records, outcome.api = len(pages), records, "rest"

    if obj.api == "rest":
        rest()
    else:
        client = BulkClient(session, session.api_version)
        try:
            if full and obj.split_full_reload:
                ranges = year_ranges(split_from, now_utc.date())
                soqls = [build_soql(obj, fields, build_where(obj, None, now_utc, True, r)) for r in ranges]
                with ThreadPoolExecutor(max_workers=SPLIT_WORKERS) as pool:
                    results = list(pool.map(lambda q: client.query_to_files(q, page_size, namer), soqls))
            else:
                results = [client.query_to_files(build_soql(obj, fields, where), page_size, namer)]
        except Exception as exc:
            if not is_unsupported_entity(exc):
                raise
            log.warning("%s: the Bulk API doesn't support this object; using REST queryAll (set api: rest)", obj.name)
            shutil.rmtree(out_dir, ignore_errors=True)
            out_dir.mkdir(parents=True)
            namer = PageNamer(out_dir)
            rest()
        else:
            for r in results:
                outcome.job_ids.append(r.job_id)
                outcome.pages += len(r.pages)
                outcome.records += r.records

    try:
        count_soql = f"SELECT COUNT() FROM {obj.name}{where}"
        outcome.sf_count = session.sf.query(count_soql, include_deleted=True)["totalSize"]
    except Exception as exc:  # the count is a warning-level check; never fail the extract on it
        log.warning("%s: COUNT() check failed: %s", obj.name, type(exc).__name__)
    log.info("%s: %d records in %d pages", obj.name, outcome.records, outcome.pages)
    return outcome
