import csv
from pathlib import Path

import pytest
import yaml

from sf_ingest.config import ConfigError, load_entity_env, load_objects, load_settings

from .conftest import CONFIG_DIR


def test_roojai_objects_match_the_design_counts():
    objects = load_objects(CONFIG_DIR, "roojai")
    assert len(objects) == 124
    assert sum(o.watermark == "CreatedDate" for o in objects) == 35
    assert all(o.is_history for o in objects if o.watermark == "CreatedDate")
    assert {o.name for o in objects if o.delete_policy == "delete"} == {
        "Car_Accessories_assets__c",
        "GA_Invoice_Header__c",
        "Payments__c",
        "QuestionResponse__c",
        "Quotation_Driver__c",
        "QuoteLineItem",
        "Third_Party__c",
    }
    assert {o.name for o in objects if o.include_created_today} == {"GroupMember", "ProcessNode", "User", "UserRole"}
    assert {o.name for o in objects if o.load_type == "full"} == {"GroupMember", "User"}
    assert {o.name for o in objects if o.api == "rest"} == {"ContractStatus", "TaskStatus"}
    assert {o.name: o.paused for o in objects if o.paused} == {"Contract": ("uat",), "Payments__c": ("uat",)}
    assert sum(o.new_fields == "require-approval" for o in objects) == 11
    assert [o.name for o in objects if o.view_filter] == []
    assert [(o.name, o.soql_filter) for o in objects if o.soql_filter] == [("Contract", "Id != '8002r000000vRMSAA2'")]
    encrypted = {o.name: o.encrypted_fields for o in objects if o.encrypted_fields}
    assert len(encrypted) == 11
    assert sum(len(v) for v in encrypted.values()) == 29


def test_special_encryption_rules():
    objects = {o.name: o for o in load_objects(CONFIG_DIR, "roojai")}
    account = objects["Account"]
    assert account.conditional_encryption.columns == ("Name", "FirstName", "LastName")
    assert "0012r00000Y6z0IAAR" in account.conditional_encryption.when
    assert objects["AccountHistory"].history_encryption == (
        "PersonEmail",
        "Phone",
        "Thai_Identification_Number__c",
        "Contact_Email_Address__c",
        "PersonMobilePhone",
    )
    assert objects["AccountHistory"].pii_columns == ("OldValue", "NewValue")


def test_seed_matches_the_design_counts_and_covers_pii():
    rows = list(csv.DictReader((CONFIG_DIR / "seed" / "roojai_fields_allowlist.csv").open()))
    assert sum(r["allowed"] == "1" for r in rows) == 5767
    assert sum(r["allowed"] == "0" for r in rows) == 558
    allowed = {(r["object"], r["field"]) for r in rows if r["allowed"] == "1"}
    for obj in load_objects(CONFIG_DIR, "roojai"):
        assert (obj.name, "Id") in allowed
        assert (obj.name, obj.watermark) in allowed
        for col in obj.encrypted_fields + (obj.conditional_encryption.columns if obj.conditional_encryption else ()):
            assert (obj.name, col) in allowed


def test_entity_env_derives_catalog():
    t = load_entity_env(CONFIG_DIR, "roojai", "prod")
    assert t.catalog == "roojai_th_prod"
    assert t.archival_user_ids == ("0052r0000018PjjAAE", "00528000001b87GAAQ")
    settings = load_settings(CONFIG_DIR, "roojai", "uat")
    assert settings.target.catalog == "roojai_th_uat"
    assert settings.object("Account").table == "account"


def _write_objects(tmp_path: Path, objects: list[dict]) -> Path:
    (tmp_path / "objects").mkdir()
    (tmp_path / "objects" / "x.yml").write_text(yaml.safe_dump({"objects": objects}))
    return tmp_path


@pytest.mark.parametrize(
    "obj, message",
    [
        ({"name": "A", "delete_policy": "purge"}, "not one of"),
        ({"name": "A", "colour": "red"}, "unknown keys"),
        ({"name": "A", "history_encryption": {"fields": ["X"]}}, "only applies"),
        (
            {"name": "A", "encrypted_fields": ["N"], "conditional_encryption": {"columns": ["N"], "when": "true"}},
            "both",
        ),
    ],
)
def test_invalid_object_config_is_rejected(tmp_path, obj, message):
    with pytest.raises(ConfigError, match=message):
        load_objects(_write_objects(tmp_path, [obj]), "x")


def test_duplicate_objects_are_rejected(tmp_path):
    with pytest.raises(ConfigError, match="duplicate"):
        load_objects(_write_objects(tmp_path, [{"name": "A"}, {"name": "A"}]), "x")


def test_objects_with_encrypted_fields_require_approval(tmp_path):
    (tmp_path / "objects").mkdir()
    (tmp_path / "objects" / "x.yml").write_text(
        yaml.safe_dump({"objects": [{"name": "Lead", "encrypted_fields": ["Email"]}]})
    )
    with pytest.raises(ConfigError, match="require-approval"):
        load_objects(tmp_path, "x")
    for obj in load_objects(CONFIG_DIR, "roojai"):
        if obj.encrypted_fields or obj.conditional_encryption:
            assert obj.new_fields == "require-approval", obj.name


def test_pii_settings_and_legacy_cipher():
    pii = load_entity_env(CONFIG_DIR, "roojai", "prod").pii
    assert (pii.key_encoding, pii.iv_encoding, pii.iv_prefixed, pii.base64_line_breaks) == (
        "base64",
        "base64",
        True,
        False,
    )
    assert (pii.legacy.key_encoding, pii.legacy.iv_encoding) == ("utf8", "hex")
    assert pii.legacy.secret_scope == "TH-BI-databricks-kv"
