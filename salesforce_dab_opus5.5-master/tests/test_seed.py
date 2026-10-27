from datetime import UTC, datetime

import pytest

from sf_ingest import ops
from sf_ingest.config import ObjectConfig
from sf_ingest.encrypt import pii_keys
from sf_ingest.metadata import FieldInfo
from sf_ingest.scd2 import UncastableValues, scd2_table
from sf_ingest.seed import match_columns, seed, to_first_versions

from .conftest import CATALOG, account_config
from .test_encrypt import legacy_ciphertext, reference_ciphertext
from .test_tasks import FakeContext

pytestmark = pytest.mark.spark

FIELDS = [
    FieldInfo("Id", "id"),
    FieldInfo("Name", "string"),
    FieldInfo("Type", "picklist"),
    FieldInfo("Phone__c", "phone"),
    FieldInfo("IsDeleted", "boolean"),
    FieldInfo("LastModifiedById", "reference"),
    FieldInfo("Missing__c", "string"),
    FieldInfo("SystemModstamp", "datetime"),
]
TYPES = {
    "id": "STRING",
    "name": "STRING",
    "type": "STRING",
    "phone__c": "STRING",
    "isdeleted": "BOOLEAN",
    "lastmodifiedbyid": "STRING",
    "missing__c": "STRING",
    "systemmodstamp": "TIMESTAMP_NTZ",
}
BACKUP_SCHEMA = "STRUCT<ID: STRING, name: STRING, Type: STRING, phone__c: STRING, IsDeleted: BOOLEAN, "
BACKUP_SCHEMA += "LastModifiedById: STRING, SystemModstamp: TIMESTAMP_NTZ>"


def backup_rows():
    return [
        # Person account: Name and Phone__c encrypted with the legacy key, one of them in the unprefixed form.
        (
            "001A",
            legacy_ciphertext("Jane Doe"),
            "Customer",
            legacy_ciphertext("0811", iv_prefixed=False),
            False,
            "005U",
            datetime(2024, 5, 1, 9, 0),
        ),
        # Business account: Name in clear; a phone from before encryption existed, still plaintext.
        ("001B", "Acme Co", "Business", "0822", True, "005ARCH", datetime(2024, 5, 2, 9, 0)),
    ]


def test_match_columns_case_insensitively():
    assert match_columns(FIELDS, ["ID", "NAME", "systemmodstamp"]) == {
        "Id": "ID",
        "Name": "NAME",
        "SystemModstamp": "systemmodstamp",
    }


def test_backup_rows_become_first_versions_re_encrypted(spark, spec, legacy_spec):
    src = spark.createDataFrame(backup_rows(), BACKUP_SCHEMA)
    mapping = match_columns(FIELDS, src.columns)
    out = {
        r.Id: r
        for r in to_first_versions(
            src, account_config(), FIELDS, mapping, TYPES, ("005ARCH",), legacy_spec, spec, 1, "s1"
        ).collect()
    }
    a, b = out["001A"], out["001B"]
    assert a.Name == reference_ciphertext("Jane Doe") and a.Phone__c == reference_ciphertext("0811")
    assert b.Name == "Acme Co"  # business accounts stay readable
    assert b.Phone__c == reference_ciphertext("0822")  # legacy plaintext gets encrypted
    assert a.SystemModstamp == datetime(2024, 5, 1, 9, 0)  # already Bangkok time: not shifted
    assert a._valid_from == a.SystemModstamp and a._is_current and a._valid_to is None
    assert a.Missing__c is None and a._run_id == "seed-s1"
    assert b._is_archived and not b._is_deleted


def test_ciphertext_the_legacy_key_cant_read_fails_with_counts(spark, spec, legacy_spec):
    rows = backup_rows()
    rows[0] = ("001A", reference_ciphertext("Jane Doe"), *rows[0][2:])  # encrypted with another key
    src = spark.createDataFrame(rows, BACKUP_SCHEMA)
    with pytest.raises(UncastableValues, match=r"don't decrypt with the legacy key.*'Name': 1"):
        to_first_versions(
            src, account_config(), FIELDS, match_columns(FIELDS, src.columns), TYPES, (), legacy_spec, spec, 1, "s1"
        )


def test_seed_loads_backup_tables_and_seeds_watermarks(spark, catalog, spec, legacy_spec):
    obj = account_config(encrypted_fields=("Phone__c",))
    ctx = FakeContext(spark, spec, [obj, ObjectConfig(name="Lead")])
    ctx._legacy = legacy_spec
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`backup_salesforce`")
    spark.sql(f"DROP TABLE IF EXISTS `{CATALOG}`.`backup_salesforce`.`account`")
    spark.createDataFrame(backup_rows(), BACKUP_SCHEMA).write.saveAsTable(f"{CATALOG}.backup_salesforce.account")
    ops.write_run_plan(spark, catalog, "r1", {"Account": [f.to_dict() for f in FIELDS], "Lead": []})

    seed(ctx, set())
    rows = spark.table(scd2_table(catalog, obj)).collect()
    assert len(rows) == 2
    # Watermark: highest SystemModstamp (Bangkok) minus 7 hours.
    assert ops.get_watermark(spark, catalog, "Account") == datetime(2024, 5, 2, 2, 0, tzinfo=UTC)
    assert ops.encryption_state(spark, catalog, "Account") == set(pii_keys(obj))
    assert any(k.startswith("Name@") for k in pii_keys(obj))  # the conditional key names its rule
    checks = {r.object: r for r in spark.table(ops.ops_table(catalog, "dq_results")).collect()}
    assert checks["Account"].passed and not checks["Lead"].passed  # Lead has no backup table

    with pytest.raises(RuntimeError, match="Seed failed"):  # never over existing data
        seed(ctx, {"Account"})


def test_seeded_records_hash_like_nightly_ones(spark, catalog, spec, legacy_spec, tmp_path):
    """B-38: the seed hashes the same stored values as the nightly load, and turns '' into null like it."""
    from sf_ingest.scd2 import load_object

    from .conftest import write_page

    obj = account_config(encrypted_fields=("Phone__c",))
    ctx = FakeContext(spark, spec, [obj])
    ctx._legacy = legacy_spec
    rows = backup_rows()
    rows[1] = (*rows[1][:3], "", *rows[1][4:])  # an empty phone in the backup
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`backup_salesforce`")
    spark.createDataFrame(rows, BACKUP_SCHEMA).write.saveAsTable(f"{CATALOG}.backup_salesforce.account")
    ops.write_run_plan(spark, catalog, "r1", {"Account": [f.to_dict() for f in FIELDS]})
    seed(ctx, set())
    table = scd2_table(catalog, obj)
    assert {r.Id: r.Phone__c for r in spark.table(table).collect()}["001B"] is None

    # The same record, unchanged, with a later SystemModstamp: same hash, so no new version.
    header = [f.name for f in FIELDS]
    page = [["001B", "Acme Co", "Business", "", "true", "005ARCH", "", "2024-05-03T02:00:00.000Z"]]
    write_page(tmp_path / "n1", 1, header, page)
    stats = load_object(spark, catalog, obj, FIELDS, tmp_path / "n1", ("005ARCH",), spec, 1, "n1")
    assert stats.versions_written == 0
