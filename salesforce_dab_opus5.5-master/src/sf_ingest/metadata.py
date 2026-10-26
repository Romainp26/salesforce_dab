"""Salesforce describe, schema drift and the new-field policy (FR-09, D-09, D-19)."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field

from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceResourceNotFound

from .config import ObjectConfig

log = logging.getLogger(__name__)

# Compound fields are skipped in favour of their components; base64 fields are excluded.
SKIP_TYPES = {"address", "location", "base64"}
# Queried whenever Salesforce has them, whatever the allowlist says: delete and archival detection read them.
SYSTEM_FIELDS = ("IsDeleted", "LastModifiedById")
_SYSTEM_LOWER = {f.lower() for f in SYSTEM_FIELDS}


@dataclass(frozen=True)
class FieldInfo:
    name: str
    sf_type: str
    length: int = 0
    precision: int = 0
    scale: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class FieldPlan:
    query_fields: list[FieldInfo] = field(default_factory=list)
    auto_added: list[FieldInfo] = field(default_factory=list)
    pending: list[FieldInfo] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    retyped: list[tuple[str, str, str]] = field(default_factory=list)


def parse_describe(describe: dict) -> list[FieldInfo]:
    return [
        FieldInfo(
            name=f["name"],
            sf_type=f["type"],
            length=int(f.get("length") or 0),
            precision=int(f.get("precision") or 0),
            scale=int(f.get("scale") or 0),
        )
        for f in describe["fields"]
    ]


def describe_all(
    sf: Salesforce, names: list[str], workers: int = 8
) -> tuple[dict[str, list[FieldInfo]], dict[str, Exception]]:
    """Describe every object. Returns (fields per object, error per object that failed).

    One object's failure never stops the others: preflight decides what it means for that object.
    """

    def one(name: str) -> tuple[str, list[FieldInfo] | None, Exception | None]:
        try:
            return name, parse_describe(getattr(sf, name).describe()), None
        except Exception as exc:
            return name, None, exc

    described, errors = {}, {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for name, fields, exc in pool.map(one, names):
            if exc is None:
                described[name] = fields
            else:
                errors[name] = exc
    return described, errors


def is_missing_object(exc: Exception) -> bool:
    """Describe's 404: the object doesn't exist in this org, e.g. a PROD-only object in a UAT sandbox."""
    return isinstance(exc, SalesforceResourceNotFound)


def plan_fields(
    obj: ObjectConfig,
    described: list[FieldInfo],
    allowlist: dict[str, int],
    previous: dict[str, str] | None,
) -> FieldPlan:
    """Decide which fields to query tonight.

    `allowlist` maps lower-cased field name to `allowed` (1 or 0); `previous` maps lower-cased field name
    to the describe type stored on the last run, or is None on the first run.
    """
    plan = FieldPlan()
    for f in described:
        if f.sf_type in SKIP_TYPES:
            continue
        allowed = allowlist.get(f.name.lower())
        if allowed == 1 or f.name.lower() in _SYSTEM_LOWER:
            plan.query_fields.append(f)
        elif allowed is None:
            # allowed = 0 is a deliberate exclusion: never queried, never reported as new (D-19).
            if obj.new_fields == "auto-add":
                plan.auto_added.append(f)
                plan.query_fields.append(f)
            else:
                plan.pending.append(f)

    queried = {f.name.lower() for f in plan.query_fields}
    for required in ("Id", obj.watermark):
        if required.lower() not in queried:
            raise ValueError(f"{obj.name}: {required} must be described and allowlisted")

    if previous is not None:
        now = {f.name.lower(): f for f in described}
        allowed_names = {k for k, v in allowlist.items() if v == 1}
        plan.removed = sorted(k for k in previous if k not in now and k in allowed_names)
        plan.retyped = sorted(
            (now[k].name, previous[k], now[k].sf_type)
            for k in previous
            if k in now and k in allowed_names and previous[k] != now[k].sf_type
        )
    return plan
