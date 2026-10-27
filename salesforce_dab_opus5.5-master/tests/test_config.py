import csv
from pathlib import Path

import pytest
import yaml

from sf_ingest.config import ConfigError, history_parent, load_approvals, load_entity_env, load_objects, load_settings

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
    # The design's 29, plus Account's two formula copies of the person phones (B-31).
    assert sum(len(v) for v in encrypted.values()) == 31
    assert {"Mobile_Formula__pc", "Phone_Formula__pc"} <= set(encrypted["Account"])


def test_special_encryption_rules():
    objects = {o.name: o for o in load_objects(CONFIG_DIR, "roojai")}
    account = objects["Account"]
    assert account.conditional_encryption.columns == ("Name", "FirstName", "LastName")
    assert "0012r00000Y6z0IAAR" in account.conditional_encryption.when
    # Its own five fields, then Account's PII fields (B-21); Thai_Identification_Number__c is in both lists.
    history = objects["AccountHistory"].history_encryption
    assert history[:5] == (
        "PersonEmail",
        "Phone",
        "Thai_Identification_Number__c",
        "Contact_Email_Address__c",
        "PersonMobilePhone",
    )
    assert set(history) == set(history[:5]) | set(account.encrypted_fields) | {"Name", "FirstName", "LastName"}
    assert len(history) == len({f.lower() for f in history})
    assert objects["AccountHistory"].pii_columns == ("OldValue", "NewValue")


def test_history_objects_of_pii_objects_encrypt_their_parents_fields():
    objects = {o.name: o for o in load_objects(CONFIG_DIR, "roojai")}
    for name, parent in [
        ("Claim__History", "Claim__c"),
        ("Payments__History", "Payments__c"),
        ("Vehicle_Inspection_Data__History", "Vehicle_Inspection_Data__c"),
        ("AWSConnectOutboundCallList__History", "AWSConnectOutboundCallList__c"),
        ("OpportunityFieldHistory", "Opportunity"),
    ]:
        assert history_parent(name) == parent
        assert set(objects[parent].encrypted_fields) <= set(objects[name].history_encryption), name
    # History objects of objects without PII stay unencrypted.
    assert objects["CaseHistory"].history_encryption == ()
    assert history_parent("Pricebook2History") == "Pricebook2" and history_parent("Account") is None


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
        ({"name": "A", "new_fields": "require-approval", "encrypted_fields": ["Id"]}, "can't be encrypted"),
        ({"name": "A", "new_fields": "require-approval", "encrypted_fields": ["isdeleted"]}, "can't be encrypted"),
        ({"name": "A", "new_fields": "require-approval", "encrypted_fields": ["CreatedDate"]}, "can't be encrypted"),
        (
            {
                "name": "A",
                "new_fields": "require-approval",
                "encrypted_fields": ["Type"],
                "conditional_encryption": {"columns": ["Name"], "when": "lower(Type) = 'x'"},
            },
            "rule reads them",
        ),
        (
            {"name": "A", "new_fields": "require-approval", "encrypted_fields": ["N"], "decrypt_fields": ["n"]},
            "decrypt_fields",
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
    assert pii.previous_keys == ()


def test_pii_keys_and_salesforce_secrets_live_in_separate_scopes(tmp_path):
    t = load_entity_env(CONFIG_DIR, "roojai", "prod")
    # Readers of the PII key never get READ on the scope holding the JWT key (B-22).
    assert (t.secret_scope, t.pii_secret_scope) == ("sf-ingest-prod", "sf-pii-roojai-prod")
    assert load_entity_env(CONFIG_DIR, "kumka", "prod").pii_secret_scope == "sf-pii-kumka-prod"
    assert t.access.pii_readers == "pii_readers_roojai_th_prod"
    assert t.access.operators == "sf_operators_roojai_th_prod"
    assert (t.reconcile_days, t.reconcile_max_deleted_share, t.freshness_hours) == (7, 0.05, 36)

    raw = yaml.safe_load((CONFIG_DIR / "entities.yml").read_text())
    raw["entities"]["roojai"]["pii_secret_scopes"]["prod"] = "sf-ingest-prod"
    (tmp_path / "entities.yml").write_text(yaml.safe_dump(raw))
    with pytest.raises(ConfigError, match="must differ"):
        load_entity_env(tmp_path, "roojai", "prod")

    raw = yaml.safe_load((CONFIG_DIR / "entities.yml").read_text())
    raw["pii"]["previous_keys"] = [{"key_version": 1, "aes_key_secret": "k", "aes_iv_secret": "iv"}]
    (tmp_path / "entities.yml").write_text(yaml.safe_dump(raw))
    with pytest.raises(ConfigError, match="previous_keys"):
        load_entity_env(tmp_path, "roojai", "prod")


def _write_approvals(tmp_path: Path, approvals: list[dict]) -> Path:
    (tmp_path / "approvals").mkdir(exist_ok=True)
    (tmp_path / "approvals" / "roojai.yml").write_text(yaml.safe_dump(approvals))
    return tmp_path


def test_approvals_come_from_config_and_pii_ones_must_be_encrypted(tmp_path):
    objects = load_objects(CONFIG_DIR, "roojai")
    assert load_approvals(CONFIG_DIR, "roojai", objects) == ()
    base = {"decided_by": "jane", "decided_on": "2026-11-02"}
    ok = [
        {"object": "Account", "field": "Tier__c", "decision": "approved", "pii": False, **base},
        {"object": "Account", "field": "Mobile_Formula__pc", "decision": "approved", "pii": True, **base},
        {"object": "Claim__c", "field": "Old__c", "decision": "rejected", **base},
    ]
    got = load_approvals(_write_approvals(tmp_path, ok), "roojai", objects)
    assert [(a.object, a.field, a.decision, a.pii) for a in got] == [
        ("Account", "Tier__c", "approved", False),
        ("Account", "Mobile_Formula__pc", "approved", True),
        ("Claim__c", "Old__c", "rejected", None),
    ]
    for bad, message in [
        ({"object": "Account", "field": "Spouse_Mobile__c", "decision": "approved", "pii": True}, "encrypted_fields"),
        ({"object": "Account", "field": "X__c", "decision": "approved"}, "pii: true or pii: false"),
        ({"object": "Nope__c", "field": "X__c", "decision": "rejected"}, "not configured"),
        ({"object": "Account", "field": "X__c", "decision": "maybe"}, "not one of"),
    ]:
        with pytest.raises(ConfigError, match=message):
            load_approvals(_write_approvals(tmp_path, [{**bad, **base}]), "roojai", objects)
