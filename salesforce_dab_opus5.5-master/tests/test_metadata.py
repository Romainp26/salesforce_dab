import pytest
from simple_salesforce.exceptions import SalesforceResourceNotFound

from sf_ingest.config import ObjectConfig
from sf_ingest.metadata import FieldInfo, describe_all, is_missing_object, parse_describe, plan_fields

DESCRIBED = [
    FieldInfo("Id", "id"),
    FieldInfo("Name", "string"),
    FieldInfo("Secret__c", "string"),
    FieldInfo("New__c", "string"),
    FieldInfo("BillingAddress", "address"),
    FieldInfo("BillingCity", "string"),
    FieldInfo("Logo__c", "base64"),
    FieldInfo("SystemModstamp", "datetime"),
]
ALLOWLIST = {"id": 1, "name": 1, "secret__c": 0, "billingcity": 1, "systemmodstamp": 1, "gone__c": 1}


def test_parse_describe():
    fields = parse_describe(
        {"fields": [{"name": "Amount", "type": "currency", "length": 0, "precision": 18, "scale": 2}]}
    )
    assert fields == [FieldInfo("Amount", "currency", 0, 18, 2)]


def test_describe_all_returns_failures_per_object_instead_of_raising():
    missing = SalesforceResourceNotFound("https://x/sobjects/Gone__c/describe", 404, "Gone__c", [])

    class FakeSf:
        def __getattr__(self, name):
            def describe():
                if name == "Gone__c":
                    raise missing
                if name == "Broken__c":
                    raise RuntimeError("503 Service Unavailable")
                return {"fields": [{"name": "Id", "type": "id"}]}

            return type("SObject", (), {"describe": staticmethod(describe)})()

    described, errors = describe_all(FakeSf(), ["Account", "Gone__c", "Broken__c"])
    assert described == {"Account": [FieldInfo("Id", "id")]}
    assert errors["Gone__c"] is missing and "503" in str(errors["Broken__c"])
    assert is_missing_object(errors["Gone__c"]) and not is_missing_object(errors["Broken__c"])


def test_auto_add_queries_new_fields_and_skips_exclusions():
    plan = plan_fields(ObjectConfig(name="Account"), DESCRIBED, ALLOWLIST, None)
    assert [f.name for f in plan.query_fields] == ["Id", "Name", "New__c", "BillingCity", "SystemModstamp"]
    assert [f.name for f in plan.auto_added] == ["New__c"]
    assert plan.pending == []
    # allowed = 0, compound and base64 fields are never queried nor reported as new (D-19).
    # An allowed field Salesforce doesn't describe is reported on the first run too, and every night (B-32).
    assert plan.removed == ["gone__c"] and plan.retyped == []


def test_require_approval_leaves_new_fields_out():
    plan = plan_fields(ObjectConfig(name="Account", new_fields="require-approval"), DESCRIBED, ALLOWLIST, None)
    assert "New__c" not in [f.name for f in plan.query_fields]
    assert [f.name for f in plan.pending] == ["New__c"]
    assert plan.auto_added == []


def test_removed_and_retyped_fields_are_reported_against_the_previous_snapshot():
    previous = {
        "id": "id",
        "name": "textarea",
        "gone__c": "string",
        "secret__c": "string",
        "systemmodstamp": "datetime",
    }
    plan = plan_fields(ObjectConfig(name="Account"), DESCRIBED, ALLOWLIST, previous)
    assert plan.removed == ["gone__c"]
    assert plan.retyped == [("Name", "textarea", "string")]
    # Reported every night, until it is described again or excluded.
    assert plan_fields(ObjectConfig(name="Account"), DESCRIBED, ALLOWLIST, {"id": "id"}).removed == ["gone__c"]


def test_precision_and_scale_changes_are_retypes():
    described = [
        FieldInfo("Id", "id"),
        FieldInfo("Amount__c", "currency", 0, 18, 4),
        FieldInfo("SystemModstamp", "datetime"),
    ]
    previous = {f.name.lower(): f for f in described}
    previous["amount__c"] = FieldInfo("Amount__c", "currency", 0, 16, 2)
    allow = {"id": 1, "amount__c": 1, "systemmodstamp": 1}
    plan = plan_fields(ObjectConfig(name="Opportunity"), described, allow, previous)
    assert plan.retyped == [("Amount__c", "currency(16,2)", "currency(18,4)")]


def test_pii_fields_must_be_described():
    obj = ObjectConfig(name="Account", new_fields="require-approval", encrypted_fields=("Phone_c",))
    # A typo, or a field hidden from the integration user, fails the object instead of storing plaintext (B-26).
    with pytest.raises(ValueError, match=r"\['Phone_c'\] are not in Salesforce's describe"):
        plan_fields(obj, DESCRIBED, ALLOWLIST, None)
    ok = ObjectConfig(name="Account", new_fields="require-approval", encrypted_fields=("secret__C",))
    assert plan_fields(ok, DESCRIBED, ALLOWLIST, None).query_fields


def test_id_and_watermark_are_required():
    with pytest.raises(ValueError, match="SystemModstamp"):
        plan_fields(ObjectConfig(name="Account"), DESCRIBED, {"id": 1, "systemmodstamp": 0}, None)


def test_delete_detection_fields_are_always_queried():
    described = [FieldInfo("Id", "id"), FieldInfo("IsDeleted", "boolean"), FieldInfo("LastModifiedById", "reference")]
    described.append(FieldInfo("SystemModstamp", "datetime"))
    allow = {"id": 1, "systemmodstamp": 1, "isdeleted": 0}  # excluded by mistake, LastModifiedById unknown
    plan = plan_fields(ObjectConfig(name="Account", new_fields="require-approval"), described, allow, None)
    assert [f.name for f in plan.query_fields] == ["Id", "IsDeleted", "LastModifiedById", "SystemModstamp"]
    assert plan.pending == [] and plan.auto_added == []
