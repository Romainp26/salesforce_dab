"""Deterministic PII encryption: AES-CBC, PKCS padding, static IV, base64 (D-07, D-20).

Every encrypt and decrypt expression is built here. The format is Spark's native one (B-13): base64 of the
16-byte IV followed by the CBC ciphertext, without line breaks, under a random 256-bit key.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import LegacyCipher, ObjectConfig, PiiSettings, PreviousKey, rule_identifiers
from .transform import BANGKOK_OFFSET_HOURS

_COND_COL = "__sf_encrypt_when"

# How a secret's text becomes key or IV bytes: base64 for the current key; the legacy key is UTF-8 text with a hex IV.
_DECODE = {"base64": "unbase64({})", "hex": "unhex({})", "utf8": "encode({}, 'UTF-8')"}


def quote(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


@dataclass(frozen=True)
class CipherSpec:
    key_sql: str  # SQL expression returning the BINARY key
    iv_sql: str  # SQL expression returning the BINARY 16-byte IV
    iv_prefixed: bool

    @classmethod
    def from_secret_scope(cls, scope: str, pii: PiiSettings) -> CipherSpec:
        """Read key and IV with secret() inside the expression, so they never appear as literals in plans."""
        return cls.from_values(
            f"secret('{scope}', '{pii.aes_key_secret}')", f"secret('{scope}', '{pii.aes_iv_secret}')", pii
        )

    @classmethod
    def legacy(cls, legacy: LegacyCipher) -> CipherSpec:
        """The key of the backup_salesforce data. Only `decrypt_any_sql` should use it."""
        scope = legacy.secret_scope
        return cls(
            key_sql=_DECODE[legacy.key_encoding].format(f"secret('{scope}', '{legacy.aes_key_secret}')"),
            iv_sql=_DECODE[legacy.iv_encoding].format(f"secret('{scope}', '{legacy.aes_iv_secret}')"),
            iv_prefixed=True,
        )

    @classmethod
    def previous(cls, scope: str, key: PreviousKey, pii: PiiSettings) -> CipherSpec:
        """A key replaced by a rotation (same scope and encodings as the current one)."""
        return cls.from_values(
            f"secret('{scope}', '{key.aes_key_secret}')", f"secret('{scope}', '{key.aes_iv_secret}')", pii
        )

    @classmethod
    def from_values(cls, key_value_sql: str, iv_value_sql: str, pii: PiiSettings) -> CipherSpec:
        """Build from SQL expressions returning the stored key and IV text, decoded per entities.yml."""
        return cls(
            key_sql=_DECODE[pii.key_encoding].format(key_value_sql),
            iv_sql=_DECODE[pii.iv_encoding].format(iv_value_sql),
            iv_prefixed=pii.iv_prefixed,
        )

    def encrypt_sql(self, value_sql: str) -> str:
        cipher = f"aes_encrypt({value_sql}, {self.key_sql}, 'CBC', 'PKCS', {self.iv_sql})"
        if not self.iv_prefixed:
            # Spark prepends the IV to CBC output (the stored format, as in salesforce_dab); dropping it only
            # serves ciphertext made elsewhere without the prefix.
            cipher = f"substr({cipher}, 17)"
        return f"base64({cipher})"

    def decrypt_sql(self, value_sql: str) -> str:
        data = f"unbase64({value_sql})" if self.iv_prefixed else f"concat({self.iv_sql}, unbase64({value_sql}))"
        return f"CAST(aes_decrypt({data}, {self.key_sql}, 'CBC', 'PKCS') AS STRING)"

    def try_decrypt_sql(self, value_sql: str) -> str:
        """Null instead of an error when the value isn't this key's ciphertext."""
        raw = f"try_to_binary({value_sql}, 'base64')"
        data = raw if self.iv_prefixed else f"concat({self.iv_sql}, {raw})"
        return f"CAST(try_aes_decrypt({data}, {self.key_sql}, 'CBC', 'PKCS') AS STRING)"

    def decrypt_any_sql(self, value_sql: str) -> str:
        """Decrypt ciphertext stored with or without the IV prefix and with or without base64 line breaks.

        Null when the value doesn't decrypt. The IV is static, so a value starting with it carries the prefix
        (a ciphertext that starts with those 16 bytes by chance has a 2^-128 probability).
        """
        raw = f"try_to_binary(regexp_replace({value_sql}, '[\\r\\n]', ''), 'base64')"
        data = f"CASE WHEN substr({raw}, 1, 16) = {self.iv_sql} THEN {raw} ELSE concat({self.iv_sql}, {raw}) END"
        return f"CAST(try_aes_decrypt({data}, {self.key_sql}, 'CBC', 'PKCS') AS STRING)"


def decrypt_at_sql(value_sql: str, version_sql: str, current: CipherSpec, previous: dict[int, CipherSpec]) -> str:
    """Decrypt with the key of the row's `_pii_key_version`: one in `pii.previous_keys` until the rotation has
    re-encrypted the row, the current one otherwise (B-27)."""
    if not previous:
        return current.decrypt_sql(value_sql)
    whens = " ".join(f"WHEN {v} THEN {spec.decrypt_sql(value_sql)}" for v, spec in sorted(previous.items()))
    return f"CASE {version_sql} {whens} ELSE {current.decrypt_sql(value_sql)} END"


def pin_base64_format(spark, pii: PiiSettings) -> None:
    """Set how base64() wraps long ciphertext for the whole session. Call once, before any encryption."""
    spark.conf.set("spark.sql.chunkBase64String.enabled", str(pii.base64_line_breaks).lower())


def is_ciphertext_sql(value_sql: str) -> str:
    """True when the value decodes from base64 to whole AES blocks; plaintext like 'John' fails."""
    raw = f"try_to_binary({value_sql}, 'base64')"
    return f"({raw} IS NOT NULL AND octet_length({raw}) > 0 AND octet_length({raw}) % 16 = 0)"


def history_pii_key(field: str) -> str:
    return f"history:{field}"


def rule_key(column: str, when: str) -> str:
    """A conditional column's key names its rule (`Name@1a2b3c4d`), so a changed rule is a new key and reaches
    the existing rows it now covers."""
    return f"{column}@{hashlib.sha256(when.encode()).hexdigest()[:8]}"


def pii_keys(obj: ObjectConfig) -> list[str]:
    """Names under which ops_salesforce.encryption_state records what is already encrypted."""
    keys = list(obj.encrypted_fields)
    if obj.conditional_encryption:
        keys += [rule_key(c, obj.conditional_encryption.when) for c in obj.conditional_encryption.columns]
    keys += [history_pii_key(f) for f in obj.history_encryption]
    return keys


def key_target(key: str) -> str:
    """What a key protects, whatever its rule: a column (`name`) or a history field (`history:phone`), lower case."""
    if key.startswith("history:"):
        return "history:" + key.removeprefix("history:").lower()
    return key.split("@")[0].lower()


def key_column(key: str) -> str:
    """The column a key rewrites: OldValue/NewValue (as `oldvalue`) for history keys."""
    return "oldvalue" if key.startswith("history:") else key.split("@")[0].lower()


def condition_fields(obj: ObjectConfig, known: list[str]) -> list[str]:
    """The fields among `known` that the conditional encryption rule reads, e.g. Id and Type for Account.

    A partial extract (a field backfill) must select them, or the rule can't be evaluated.
    """
    if not obj.conditional_encryption:
        return []
    words = rule_identifiers(obj.conditional_encryption.when)
    return [f for f in known if f.lower() in words]


def has_history_values(columns: set[str]) -> bool:
    """OldValue/NewValue/Field exist: OpportunityHistory, for one, has none, so history rules don't apply to it."""
    return {"field", "oldvalue", "newvalue"} <= columns


def _sql_list(values) -> str:
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


# Names a field takes in `*History.Field` besides its own: AccountHistory records Name changes as `TextName`
# (docs/uat-checklist.md checks the values each org writes).
HISTORY_FIELD_ALIASES = {"name": ("textname",)}


def history_match_sql(fields) -> str:
    """`Field` is one of the history fields or one of their aliases; case-insensitive, like every other field name
    match."""
    names = {f.lower() for f in fields}
    names |= {alias for f in names for alias in HISTORY_FIELD_ALIASES.get(f, ())}
    return f"lower(Field) IN ({_sql_list(sorted(names))})"


def pii_predicates(obj: ObjectConfig, columns: set[str]) -> list[tuple[str, str]]:
    """(column, SQL condition on the rows where it holds ciphertext) for every PII rule of the object."""
    preds = [(c, "true") for c in obj.encrypted_fields if c.lower() in columns]
    if obj.conditional_encryption:
        when = f"coalesce(({obj.conditional_encryption.when}), false)"
        preds += [(c, when) for c in obj.conditional_encryption.columns if c.lower() in columns]
    if obj.history_encryption and has_history_values(columns):
        preds += [(c, history_match_sql(obj.history_encryption)) for c in ("OldValue", "NewValue")]
    return preds


def encrypt_columns(df: DataFrame, obj: ObjectConfig, spec: CipherSpec) -> DataFrame:
    """Encrypt the object's PII columns in a DataFrame of plaintext strings."""
    present = {c.lower(): c for c in df.columns}

    def enc(name: str):
        return F.expr(spec.encrypt_sql(quote(name)))

    cond = obj.conditional_encryption
    cond_present = bool(cond) and any(c.lower() in present for c in cond.columns)
    if cond_present:
        # Evaluate the rule on plaintext before any column changes (config validation also keeps the columns it
        # reads out of the PII config, B-26).
        df = df.withColumn(_COND_COL, F.coalesce(F.expr(cond.when), F.lit(False)))

    for field in obj.encrypted_fields:
        if col := present.get(field.lower()):
            df = df.withColumn(col, enc(col))

    if cond_present:
        for field in cond.columns:
            if col := present.get(field.lower()):
                df = df.withColumn(col, F.when(F.col(_COND_COL), enc(col)).otherwise(F.col(col)))
        df = df.drop(_COND_COL)

    if obj.history_encryption and has_history_values(set(present)):
        match = F.lower(F.col(present["field"])).isin([f.lower() for f in obj.history_encryption])
        for name in ("oldvalue", "newvalue"):
            col = present[name]
            df = df.withColumn(col, F.when(match, enc(col)).otherwise(F.col(col)))
    return df


def raw_text_sql(col_sql: str, stored_type: str) -> str:
    """The text Bulk API 2.0 CSV gives for a value now stored typed, so that a column encrypted in place matches the
    ciphertext of later loads (which encrypt the raw landed text, B-24).

    Exact for dates, datetimes (stored in Bangkok time, landed in UTC), booleans and integers. Decimals drop trailing
    zeros; open question Q12 confirms Salesforce's number format in UAT.
    """
    t = stored_type.lower()
    if t == "string":
        return col_sql
    if t == "date":
        return f"date_format({col_sql}, 'yyyy-MM-dd')"
    if t == "timestamp_ntz":
        return f"date_format({col_sql} - INTERVAL {BANGKOK_OFFSET_HOURS} HOURS, \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")"
    if t == "boolean":
        return f"lower(CAST({col_sql} AS STRING))"
    if t.startswith("decimal"):
        text = f"CAST({col_sql} AS STRING)"
        return f"regexp_replace(regexp_replace({text}, '(\\\\.[0-9]*?)0+$', '$1'), '\\\\.$', '')"
    return f"CAST({col_sql} AS STRING)"


def encrypt_change(
    obj: ObjectConfig, key: str, spec: CipherSpec, stored_type: str = "string"
) -> tuple[dict[str, str], str]:
    """({column: new value SQL}, WHERE) that encrypts the existing plaintext of one PII key in every version.

    A conditional key encrypts the rows its rule covers that aren't ciphertext yet: after a rule change, rows the
    old rule already encrypted stay as they are.
    """
    if key.startswith("history:"):
        field = key.removeprefix("history:")
        return (
            {"OldValue": spec.encrypt_sql("OldValue"), "NewValue": spec.encrypt_sql("NewValue")},
            history_match_sql([field]),
        )
    name = key.split("@")[0]
    col = quote(name)
    where = f"{col} IS NOT NULL"
    if "@" in key and obj.conditional_encryption:
        where += f" AND coalesce(({obj.conditional_encryption.when}), false) AND NOT {is_ciphertext_sql(col)}"
    return {name: spec.encrypt_sql(raw_text_sql(col, stored_type))}, where


def decrypt_change(key: str, spec: CipherSpec) -> tuple[dict[str, str], str]:
    """({column: new value SQL}, WHERE) that decrypts one PII key in every version (B-24). A value that isn't this
    key's ciphertext (a business-account Name, a plaintext left by an older run) stays as it is."""

    def dec(col_sql: str) -> str:
        return f"coalesce(CASE WHEN {is_ciphertext_sql(col_sql)} THEN {spec.try_decrypt_sql(col_sql)} END, {col_sql})"

    if key.startswith("history:"):
        field = key.removeprefix("history:")
        return {"OldValue": dec("OldValue"), "NewValue": dec("NewValue")}, history_match_sql([field])
    name = key.split("@")[0]
    col = quote(name)
    return {name: dec(col)}, f"{col} IS NOT NULL"


def rotate_change(keys, columns: set[str], old: CipherSpec, new: CipherSpec) -> dict[str, str]:
    """{column: new value SQL} re-encrypting, from `old` to `new`, every value of the PII keys recorded as encrypted
    (B-27). The keys come from ops_salesforce.encryption_state, not the config: a key waiting to be decrypted is
    rotated too, one waiting to be encrypted is left to the nightly load. A value `old` doesn't decrypt (plaintext,
    a business-account name) stays as it is."""

    def rotate(col_sql: str) -> str:
        plain = old.try_decrypt_sql(col_sql)
        return (
            f"CASE WHEN {col_sql} IS NOT NULL AND {is_ciphertext_sql(col_sql)} AND {plain} IS NOT NULL "
            f"THEN {new.encrypt_sql(plain)} ELSE {col_sql} END"
        )

    out: dict[str, str] = {}
    history = sorted({k.removeprefix("history:") for k in keys if k.startswith("history:")}, key=str.lower)
    if history and has_history_values(columns):
        for col in ("OldValue", "NewValue"):
            out[col] = f"CASE WHEN {history_match_sql(history)} THEN {rotate(col)} ELSE {col} END"
    for key in sorted(k for k in keys if not k.startswith("history:")):
        name = key.split("@")[0]
        if name.lower() in columns:
            out[name] = rotate(quote(name))
    return out
