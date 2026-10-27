"""Load and validate the bundle configuration (config/entities.yml and config/objects/<entity>.yml)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from pathlib import Path

import yaml

SCD2_SCHEMA = "bronze_salesforce_scd2"
VIEW_SCHEMA = "bronze_salesforce"
OPS_SCHEMA = "ops_salesforce"
# The manual deep clone of the Synapse tables, read once by the seed.
BACKUP_SCHEMA = "backup_salesforce"

API_VALUES = {"bulk", "rest"}
LOAD_TYPES = {"incremental", "full"}
DELETE_POLICIES = {"retain", "delete"}
NEW_FIELD_POLICIES = {"auto-add", "require-approval"}
WATERMARKS = {"SystemModstamp", "CreatedDate"}
SECRET_ENCODINGS = {"base64", "hex", "utf8"}
ENVIRONMENTS = {"uat", "prod"}
DECISIONS = {"approved", "rejected"}
# Fields the pipeline computes on (keys, watermarks, delete and archival detection, created-today views): never PII.
RESERVED_FIELDS = {"id", "isdeleted", "lastmodifiedbyid", "systemmodstamp", "createddate"}


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class ConditionalEncryption:
    columns: tuple[str, ...]
    when: str


def rule_identifiers(when: str) -> set[str]:
    """Lower-cased identifiers a SQL rule reads, outside string literals: e.g. {'id', 'type', 'lower', ...}."""
    without_strings = re.sub(r"'(?:[^']|'')*'", "''", when)
    return {w.lower() for w in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", without_strings)}


def history_parent(name: str) -> str | None:
    """The object whose field changes a *History object records: AccountHistory -> Account, Claim__History ->
    Claim__c, OpportunityFieldHistory -> Opportunity."""
    if name == "OpportunityFieldHistory":
        return "Opportunity"
    if name.endswith("__History"):
        return name.removesuffix("__History") + "__c"
    if name.endswith("History"):
        return name.removesuffix("History") or None
    return None


@dataclass(frozen=True)
class ObjectConfig:
    name: str
    watermark: str = "SystemModstamp"
    api: str = "bulk"
    load_type: str = "incremental"
    delete_policy: str = "retain"
    include_created_today: bool = False
    view_filter: str | None = None
    # SOQL condition added to every query of the object. Only for records Salesforce can't return (the query fails);
    # business exclusions belong in view_filter so SCD2 stays a full copy (D-17).
    soql_filter: str | None = None
    new_fields: str = "auto-add"
    encrypted_fields: tuple[str, ...] = ()
    conditional_encryption: ConditionalEncryption | None = None
    history_encryption: tuple[str, ...] = ()
    hash_exclude: tuple[str, ...] = ()
    split_full_reload: bool = False
    # Environments where the nightly run skips the object, e.g. bad data in one org. Naming it in the `objects`
    # job parameter still runs it, for debugging.
    paused: tuple[str, ...] = ()
    # Fields removed from the PII config whose existing ciphertext is decrypted in place (B-24). Without this
    # acknowledgement a field dropped from encrypted_fields fails the object instead.
    decrypt_fields: tuple[str, ...] = ()
    # No longer ingested: not described or loaded, its view is dropped, its SCD2 table kept (B-30).
    retired: bool = False

    @property
    def table(self) -> str:
        return self.name.lower()

    @property
    def is_history(self) -> bool:
        """`*History` objects are append-only: they never change once written."""
        return self.name.endswith("History")

    @property
    def pii_columns(self) -> tuple[str, ...]:
        """Every column that can hold ciphertext."""
        cols = list(self.encrypted_fields)
        if self.conditional_encryption:
            cols += self.conditional_encryption.columns
        if self.history_encryption:
            cols += ["OldValue", "NewValue"]
        return tuple(dict.fromkeys(cols))


@dataclass(frozen=True)
class LegacyCipher:
    """The key that encrypted the backup_salesforce data, read only by the seed to decrypt it (B-13)."""

    secret_scope: str
    key_encoding: str
    iv_encoding: str
    aes_key_secret: str
    aes_iv_secret: str


@dataclass(frozen=True)
class PreviousKey:
    """A key replaced by a rotation: the rotate task decrypts rows still carrying its version (B-27)."""

    key_version: int
    aes_key_secret: str
    aes_iv_secret: str


@dataclass(frozen=True)
class PiiSettings:
    key_version: int
    key_encoding: str
    iv_encoding: str
    iv_prefixed: bool
    # Spark's base64() breaks its output with CRLF every 76 characters unless spark.sql.chunkBase64String.enabled
    # is false. Pinned here so a platform default can never change the ciphertext.
    base64_line_breaks: bool
    aes_key_secret: str
    aes_iv_secret: str
    legacy: LegacyCipher | None = None
    previous_keys: tuple[PreviousKey, ...] = ()


@dataclass(frozen=True)
class AccessGroups:
    """Unity Catalog groups preflight grants to, per catalog (B-29)."""

    readers: str
    history_readers: str
    operators: str
    pii_readers: str


@dataclass(frozen=True)
class Approval:
    """A decision on a new field, reviewed by pull request in config/approvals/<entity>.yml (B-28)."""

    object: str
    field: str
    decision: str
    pii: bool | None
    decided_by: str
    decided_on: str


@dataclass(frozen=True)
class EntityEnv:
    """One entity in one environment: one Salesforce org and one catalog."""

    entity: str
    env: str
    catalog: str
    domain: str
    username: str
    # Salesforce JWT secrets: readable by the service principal only (B-22).
    secret_scope: str
    # AES key and IV of this entity: the service principal and pii_readers_<catalog> (B-22).
    pii_secret_scope: str
    consumer_key_secret: str
    private_key_secret: str
    private_key_password_secret: str | None
    archival_user_ids: tuple[str, ...]
    api_version: str
    bulk_page_size: int
    full_reload_split_from: str
    pii: PiiSettings
    access: AccessGroups
    # Incremental objects are checked for hard deletes (an Id-only extract) every `reconcile_days` (0: never, B-25);
    # a check that would flag more than `reconcile_max_deleted_share` of the current records fails instead.
    reconcile_days: int = 7
    reconcile_max_deleted_share: float = 0.05
    # An object with no successful ingest for this long raises `object_freshness` (B-23).
    freshness_hours: int = 36


@dataclass(frozen=True)
class Settings:
    target: EntityEnv
    objects: tuple[ObjectConfig, ...] = field(default_factory=tuple)
    approvals: tuple[Approval, ...] = field(default_factory=tuple)

    def object(self, name: str) -> ObjectConfig:
        for obj in self.objects:
            if obj.name == name:
                return obj
        raise ConfigError(f"Object {name!r} is not configured for {self.target.entity}")


def _require(mapping: dict, key: str, where: str):
    if key not in mapping or mapping[key] in (None, ""):
        raise ConfigError(f"{where}: missing {key!r}")
    return mapping[key]


def _one_of(value, allowed: set[str], where: str) -> str:
    if value not in allowed:
        raise ConfigError(f"{where}: {value!r} is not one of {sorted(allowed)}")
    return value


def load_entity_env(config_dir: Path, entity: str, env: str) -> EntityEnv:
    raw = yaml.safe_load((config_dir / "entities.yml").read_text())
    entities = _require(raw, "entities", "entities.yml")
    if entity not in entities:
        raise ConfigError(f"entities.yml: unknown entity {entity!r}")
    ent = entities[entity]
    where = f"entities.yml:{entity}"
    orgs = _require(ent, "orgs", where)
    if env not in orgs:
        raise ConfigError(f"{where}: no org for environment {env!r}")
    org = orgs[env]
    envs = _require(raw, "environments", "entities.yml")
    if env not in envs:
        raise ConfigError(f"entities.yml: unknown environment {env!r}")
    secrets = _require(ent, "secrets", where)
    pii = _require(raw, "pii", "entities.yml")
    country = _require(ent, "country", where)
    catalog = f"{entity}_{country}_{env}"
    pii_scopes = _require(ent, "pii_secret_scopes", where)
    pii_scope = _require(pii_scopes, env, f"{where}.pii_secret_scopes")
    sf_scope = _require(envs[env], "secret_scope", f"entities.yml:environments.{env}")
    if pii_scope == sf_scope:
        # Readers of the PII key must never be able to read the Salesforce JWT key (B-22).
        raise ConfigError(f"{where}.pii_secret_scopes.{env}: must differ from environments.{env}.secret_scope")
    access = _require(raw, "access", "entities.yml")
    reconcile = raw.get("reconcile") or {}
    return EntityEnv(
        entity=entity,
        env=env,
        # D-10: one catalog per entity, country and environment.
        catalog=catalog,
        domain=_require(org, "domain", f"{where}.orgs.{env}"),
        username=_require(org, "username", f"{where}.orgs.{env}"),
        secret_scope=sf_scope,
        pii_secret_scope=pii_scope,
        consumer_key_secret=_require(secrets, "consumer_key", f"{where}.secrets"),
        private_key_secret=_require(secrets, "private_key", f"{where}.secrets"),
        private_key_password_secret=secrets.get("private_key_password"),
        archival_user_ids=tuple(ent.get("archival_user_ids") or ()),
        api_version=str(_require(raw, "api_version", "entities.yml")),
        bulk_page_size=int(raw.get("bulk_page_size", 100_000)),
        full_reload_split_from=str(raw.get("full_reload_split_from", "2010-01-01")),
        pii=PiiSettings(
            key_version=int(_require(pii, "key_version", "entities.yml:pii")),
            key_encoding=_one_of(pii.get("key_encoding"), SECRET_ENCODINGS, "entities.yml:pii.key_encoding"),
            iv_encoding=_one_of(pii.get("iv_encoding"), SECRET_ENCODINGS, "entities.yml:pii.iv_encoding"),
            iv_prefixed=bool(pii.get("iv_prefixed", True)),
            base64_line_breaks=bool(pii.get("base64_line_breaks", False)),
            aes_key_secret=_require(pii, "aes_key_secret", "entities.yml:pii"),
            aes_iv_secret=_require(pii, "aes_iv_secret", "entities.yml:pii"),
            legacy=_legacy_cipher(pii.get("legacy"), pii_scope),
            previous_keys=_previous_keys(pii.get("previous_keys"), int(pii["key_version"])),
        ),
        access=AccessGroups(
            **{
                k: _require(access, k, "entities.yml:access").format(catalog=catalog)
                for k in AccessGroups.__annotations__
            }
        ),
        reconcile_days=int(reconcile.get("every_days", 7)),
        reconcile_max_deleted_share=float(reconcile.get("max_deleted_share", 0.05)),
        freshness_hours=int(raw.get("freshness_hours", 36)),
    )


def _previous_keys(raw: list | None, current: int) -> tuple[PreviousKey, ...]:
    where = "entities.yml:pii.previous_keys"
    keys = tuple(
        PreviousKey(
            key_version=int(_require(k, "key_version", where)),
            aes_key_secret=_require(k, "aes_key_secret", where),
            aes_iv_secret=_require(k, "aes_iv_secret", where),
        )
        for k in raw or ()
    )
    versions = [k.key_version for k in keys]
    if current in versions or len(set(versions)) != len(versions):
        raise ConfigError(f"{where}: versions must be distinct and differ from pii.key_version")
    return keys


def _legacy_cipher(raw: dict | None, default_scope: str) -> LegacyCipher | None:
    if not raw:
        return None
    where = "entities.yml:pii.legacy"
    return LegacyCipher(
        secret_scope=raw.get("secret_scope") or default_scope,
        key_encoding=_one_of(raw.get("key_encoding"), SECRET_ENCODINGS, f"{where}.key_encoding"),
        iv_encoding=_one_of(raw.get("iv_encoding"), SECRET_ENCODINGS, f"{where}.iv_encoding"),
        aes_key_secret=_require(raw, "aes_key_secret", where),
        aes_iv_secret=_require(raw, "aes_iv_secret", where),
    )


def _parse_object(raw: dict, defaults: dict) -> ObjectConfig:
    merged = {**defaults, **raw}
    name = _require(merged, "name", "objects")
    where = f"objects:{name}"
    known = set(ObjectConfig.__dataclass_fields__)
    unknown = set(merged) - known
    if unknown:
        raise ConfigError(f"{where}: unknown keys {sorted(unknown)}")

    cond = merged.get("conditional_encryption")
    if cond is not None:
        cond = ConditionalEncryption(
            columns=tuple(_require(cond, "columns", f"{where}.conditional_encryption")),
            when=_require(cond, "when", f"{where}.conditional_encryption"),
        )
    hist = merged.get("history_encryption")
    hist_fields = tuple(_require(hist, "fields", f"{where}.history_encryption")) if hist else ()

    obj = ObjectConfig(
        name=name,
        watermark=_one_of(merged.get("watermark", "SystemModstamp"), WATERMARKS, where),
        api=_one_of(merged.get("api", "bulk"), API_VALUES, where),
        load_type=_one_of(merged.get("load_type", "incremental"), LOAD_TYPES, where),
        delete_policy=_one_of(merged.get("delete_policy", "retain"), DELETE_POLICIES, where),
        include_created_today=bool(merged.get("include_created_today", False)),
        view_filter=merged.get("view_filter") or None,
        soql_filter=merged.get("soql_filter") or None,
        new_fields=_one_of(merged.get("new_fields", "auto-add"), NEW_FIELD_POLICIES, where),
        encrypted_fields=tuple(merged.get("encrypted_fields") or ()),
        conditional_encryption=cond,
        history_encryption=hist_fields,
        hash_exclude=tuple(merged.get("hash_exclude") or ()),
        split_full_reload=bool(merged.get("split_full_reload", False)),
        paused=tuple(_one_of(e, ENVIRONMENTS, f"{where}.paused") for e in merged.get("paused") or ()),
        decrypt_fields=tuple(merged.get("decrypt_fields") or ()),
        retired=bool(merged.get("retired", False)),
    )
    if obj.history_encryption and not obj.is_history:
        raise ConfigError(f"{where}: history_encryption only applies to *History objects")
    if cond and set(cond.columns) & set(obj.encrypted_fields):
        raise ConfigError(f"{where}: a column can't be in both encrypted_fields and conditional_encryption")
    pii = {c.lower() for c in (*obj.encrypted_fields, *(cond.columns if cond else ()))}
    if reserved := sorted(pii & (RESERVED_FIELDS | {obj.watermark.lower()})):
        raise ConfigError(f"{where}: {reserved} can't be encrypted: the pipeline keys, versions or filters on them")
    if cond and (read := sorted(pii & rule_identifiers(cond.when))):
        raise ConfigError(f"{where}: {read} can't be encrypted: the conditional_encryption rule reads them")
    if both := sorted(pii & {c.lower() for c in obj.decrypt_fields}):
        raise ConfigError(f"{where}: {both} can't be in both decrypt_fields and the PII config")
    if (obj.encrypted_fields or cond) and obj.new_fields != "require-approval":
        # A new field on an object that holds PII may be PII too: someone decides before it is ever stored.
        raise ConfigError(f"{where}: objects with encrypted fields must use new_fields: require-approval")
    return obj


def load_objects(config_dir: Path, entity: str) -> tuple[ObjectConfig, ...]:
    path = config_dir / "objects" / f"{entity}.yml"
    raw = yaml.safe_load(path.read_text())
    defaults = raw.get("defaults") or {}
    objects = tuple(_parse_object(o, defaults) for o in _require(raw, "objects", str(path)))
    names = [o.name for o in objects]
    dupes = {n for n in names if names.count(n) > 1}
    if dupes:
        raise ConfigError(f"{path}: duplicate objects {sorted(dupes)}")
    return tuple(_with_parent_pii(o, objects) for o in objects)


def _with_parent_pii(obj: ObjectConfig, objects: tuple[ObjectConfig, ...]) -> ObjectConfig:
    """A *History object encrypts OldValue/NewValue for every PII field of its parent, on top of its own
    history_encryption, and decrypts them for the parent's decrypt_fields (B-21)."""
    parent_name = history_parent(obj.name) if obj.is_history else None
    parent = next((o for o in objects if parent_name and o.name.lower() == parent_name.lower()), None)
    if parent is None:
        return obj
    cond = parent.conditional_encryption.columns if parent.conditional_encryption else ()
    fields = _dedupe((*obj.history_encryption, *parent.encrypted_fields, *cond))
    decrypt = _dedupe((*obj.decrypt_fields, *parent.decrypt_fields))
    decrypt = tuple(f for f in decrypt if f.lower() not in {x.lower() for x in fields})
    return replace(obj, history_encryption=fields, decrypt_fields=decrypt)


def _dedupe(names) -> tuple[str, ...]:
    seen, out = set(), []
    for n in names:
        if n.lower() not in seen:
            seen.add(n.lower())
            out.append(n)
    return tuple(out)


def load_approvals(config_dir: Path, entity: str, objects: tuple[ObjectConfig, ...]) -> tuple[Approval, ...]:
    """Decisions on new fields (B-28). An approved PII field must already be in its object's PII config."""
    path = config_dir / "approvals" / f"{entity}.yml"
    if not path.exists():
        return ()
    by_name = {o.name.lower(): o for o in objects}
    out = []
    for raw in yaml.safe_load(path.read_text()) or []:
        where = f"{path.name}:{raw.get('object')}.{raw.get('field')}"
        name, field_name = _require(raw, "object", where), _require(raw, "field", where)
        obj = by_name.get(name.lower())
        if obj is None:
            raise ConfigError(f"{where}: object is not configured")
        decision = _one_of(raw.get("decision"), DECISIONS, f"{where}.decision")
        pii = raw.get("pii")
        if decision == "approved":
            if not isinstance(pii, bool):
                raise ConfigError(f"{where}: an approval must say pii: true or pii: false")
            if pii and field_name.lower() not in {c.lower() for c in obj.pii_columns}:
                raise ConfigError(f"{where}: approved as PII, so list it in the object's encrypted_fields first")
        out.append(
            Approval(
                object=obj.name,
                field=field_name,
                decision=decision,
                pii=pii if isinstance(pii, bool) else None,
                decided_by=_require(raw, "decided_by", where),
                decided_on=str(_require(raw, "decided_on", where)),
            )
        )
    keys = [(a.object.lower(), a.field.lower()) for a in out]
    if dupes := sorted({k for k in keys if keys.count(k) > 1}):
        raise ConfigError(f"{path}: several decisions for {dupes}")
    return tuple(out)


def load_settings(config_dir: str | Path, entity: str, env: str) -> Settings:
    config_dir = Path(config_dir)
    objects = load_objects(config_dir, entity)
    return Settings(
        target=load_entity_env(config_dir, entity, env),
        objects=objects,
        approvals=load_approvals(config_dir, entity, objects),
    )
