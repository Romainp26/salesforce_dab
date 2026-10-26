"""Bootstrap config/objects/<entity>.yml and config/seed/<entity>_fields_allowlist.csv from the allowlist CSVs.

One-off: once generated, the YAML is the source of truth and changes go through pull requests.
The script refuses to overwrite an existing file unless --force is given.

    uv run scripts/generate_config.py --entity roojai \
        --objects-csv shared_folder/objects_allowlist_and_watermarks.csv \
        --fields-csv shared_folder/fields_allowlist.csv \
        --encrypted shared_folder/encrypted_fields.txt
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import date
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]

# Objects that Bulk API 2.0 queries don't support; read through REST queryAll.
REST_ONLY = {"ContractStatus", "TaskStatus"}

DEFAULTS = {
    "api": "bulk",
    "watermark": "SystemModstamp",
    "load_type": "incremental",
    "delete_policy": "retain",
    "include_created_today": False,
    "new_fields": "auto-add",
    # SystemModstamp changes on every extract, so it can't signal a meaningful change.
    "hash_exclude": ["SystemModstamp", "LastViewedDate", "LastReferencedDate"],
}

NEW_FIELDS = {"auto-add": "auto-add", "require approval": "require-approval"}


# The CSV puts Contract's corrupted-record filter (Id prefix 800) on the Contact row.
SPECIFIC_CONDITION_OBJECT = {"Contact": "Contract"}


def _soql_filter(raw: str) -> str | None:
    """Turn the CSV's `Id != "x"` into SOQL with a string literal."""
    raw = raw.strip()
    if not raw:
        return None
    return raw.replace('"', "'")


def build_objects(objects_csv: Path, encrypted: dict[str, list[str]]) -> list[dict]:
    rows = list(csv.DictReader(objects_csv.open(encoding="utf-8-sig")))
    out = []
    conditions = {
        SPECIFIC_CONDITION_OBJECT.get(r["TableName"].strip(), r["TableName"].strip()): r["Specific condition"]
        for r in rows
        if r["Specific condition"].strip()
    }
    for row in sorted(rows, key=lambda r: r["TableName"].lower()):
        name = row["TableName"].strip()
        obj: dict = {"name": name}
        if name in REST_ONLY:
            obj["api"] = "rest"
        if row["WatermarkColumnName"] != DEFAULTS["watermark"]:
            obj["watermark"] = row["WatermarkColumnName"]
        if row["load_type"] != DEFAULTS["load_type"]:
            obj["load_type"] = row["load_type"]
        if row["delete_policy"] == "1":
            obj["delete_policy"] = "delete"
        if row["Include CreatedDate = TODAY"] == "1":
            obj["include_created_today"] = True
        if sf := _soql_filter(conditions.get(name, "")):
            obj["soql_filter"] = sf
        new_fields = NEW_FIELDS[row["schema_evolution"].strip()]
        if new_fields != DEFAULTS["new_fields"]:
            obj["new_fields"] = new_fields
        if name in encrypted:
            obj["encrypted_fields"] = encrypted[name]
        out.append(obj)
    unknown = set(encrypted) - {o["name"] for o in out}
    if unknown:
        sys.exit(f"Encrypted fields given for unconfigured objects: {sorted(unknown)}")
    return out


def check_encrypted_allowed(fields_csv: Path, encrypted: dict[str, list[str]]) -> None:
    allowed = {
        (r["object"], r["name"]) for r in csv.DictReader(fields_csv.open(encoding="utf-8-sig")) if r["allowed"] == "1"
    }
    missing = [(o, f) for o, fs in encrypted.items() for f in fs if (o, f) not in allowed]
    if missing:
        sys.exit(f"Encrypted fields not on the allowlist: {missing}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--entity", required=True)
    p.add_argument("--objects-csv", type=Path, required=True)
    p.add_argument("--fields-csv", type=Path, required=True)
    p.add_argument("--encrypted", type=Path, required=True, help="YAML list of {name, encrypted_fields}")
    p.add_argument("--force", action="store_true")
    args = p.parse_args()

    objects_out = ROOT / "config" / "objects" / f"{args.entity}.yml"
    seed_out = ROOT / "config" / "seed" / f"{args.entity}_fields_allowlist.csv"
    for path in (objects_out, seed_out):
        if path.exists() and not args.force:
            sys.exit(f"{path} exists; edit it directly or pass --force to regenerate")

    encrypted = {e["name"]: list(e["encrypted_fields"]) for e in yaml.safe_load(args.encrypted.read_text())}
    check_encrypted_allowed(args.fields_csv, encrypted)
    objects = build_objects(args.objects_csv, encrypted)

    header = (
        f"# Object configuration for {args.entity}. Generated on {date.today()} by scripts/generate_config.py\n"
        f"# from {args.objects_csv.name} and {args.encrypted.name}; edit by pull request from now on.\n"
        "# Keys: see ObjectConfig in src/sf_ingest/config.py. Objects list only what differs from defaults.\n"
    )
    body = yaml.safe_dump(
        {"defaults": DEFAULTS, "objects": objects}, sort_keys=False, default_flow_style=None, width=110
    )
    objects_out.write_text(header + body)

    with args.fields_csv.open(encoding="utf-8-sig") as src, seed_out.open("w", newline="") as dst:
        writer = csv.writer(dst, lineterminator="\n")
        writer.writerow(["object", "field", "allowed"])
        for r in sorted(csv.DictReader(src), key=lambda r: (r["object"].lower(), r["name"].lower())):
            writer.writerow([r["object"], r["name"], r["allowed"]])

    print(f"Wrote {objects_out.relative_to(ROOT)} ({len(objects)} objects) and {seed_out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
