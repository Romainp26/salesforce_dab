"""Deterministic PII encryption: AES-CBC, PKCS padding, static IV, base64 (D-07, D-20).

Every encrypt and decrypt expression is built here. The format is Spark's native one (B-13): base64 of the
16-byte IV followed by the CBC ciphertext, without line breaks, under a random 256-bit key.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import LegacyCipher, ObjectConfig, PiiSettings

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

    def decrypt_any_sql(self, value_sql: str) -> str:
        """Decrypt ciphertext stored with or without the IV prefix and with or without base64 line breaks.

        Null when the value doesn't decrypt. The IV is static, so a value starting with it carries the prefix
        (a ciphertext that starts with those 16 bytes by chance has a 2^-128 probability).
        """
        raw = f"try_to_binary(regexp_replace({value_sql}, '[\\r\\n]', ''), 'base64')"
        data = f"CASE WHEN substr({raw}, 1, 16) = {self.iv_sql} THEN {raw} ELSE concat({self.iv_sql}, {raw}) END"
        return f"CAST(try_aes_decrypt({data}, {self.key_sql}, 'CBC', 'PKCS') AS STRING)"


def pin_base64_format(spark, pii: PiiSettings) -> None:
    """Set how base64() wraps long ciphertext for the whole session. Call once, before any encryption."""
    spark.conf.set("spark.sql.chunkBase64String.enabled", str(pii.base64_line_breaks).lower())


def is_ciphertext_sql(value_sql: str) -> str:
    """True when the value decodes from base64 to whole AES blocks; plaintext like 'John' fails."""
    raw = f"try_to_binary({value_sql}, 'base64')"
    return f"({raw} IS NOT NULL AND octet_length({raw}) > 0 AND octet_length({raw}) % 16 = 0)"


def history_pii_key(field: str) -> str:
    return f"history:{field}"


def pii_keys(obj: ObjectConfig) -> list[str]:
    """Names under which ops_salesforce.encryption_state records what is already encrypted."""
    keys = list(obj.encrypted_fields)
    if obj.conditional_encryption:
        keys += obj.conditional_encryption.columns
    keys += [history_pii_key(f) for f in obj.history_encryption]
    return keys


def condition_fields(obj: ObjectConfig, known: list[str]) -> list[str]:
    """The fields among `known` that the conditional encryption rule reads, e.g. Id and Type for Account.

    A partial extract (a field backfill) must select them, or the rule can't be evaluated.
    """
    if not obj.conditional_encryption:
        return []
    without_strings = re.sub(r"'(?:[^']|'')*'", "''", obj.conditional_encryption.when)
    words = {w.lower() for w in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", without_strings)}
    return [f for f in known if f.lower() in words]


def pii_predicates(obj: ObjectConfig, columns: set[str]) -> list[tuple[str, str]]:
    """(column, SQL condition on the rows where it holds ciphertext) for every PII rule of the object."""
    preds = [(c, "true") for c in obj.encrypted_fields if c.lower() in columns]
    if obj.conditional_encryption:
        when = f"coalesce(({obj.conditional_encryption.when}), false)"
        preds += [(c, when) for c in obj.conditional_encryption.columns if c.lower() in columns]
    if obj.history_encryption:
        fields = ", ".join("'" + f.replace("'", "''") + "'" for f in obj.history_encryption)
        preds += [(c, f"Field IN ({fields})") for c in ("OldValue", "NewValue")]
    return preds


def encrypt_columns(df: DataFrame, obj: ObjectConfig, spec: CipherSpec) -> DataFrame:
    """Encrypt the object's PII columns in a DataFrame of plaintext strings."""
    present = {c.lower(): c for c in df.columns}

    def enc(name: str):
        return F.expr(spec.encrypt_sql(quote(name)))

    for field in obj.encrypted_fields:
        if col := present.get(field.lower()):
            df = df.withColumn(col, enc(col))

    cond = obj.conditional_encryption
    if cond and any(c.lower() in present for c in cond.columns):
        # Evaluate the rule on plaintext before any column changes.
        df = df.withColumn(_COND_COL, F.coalesce(F.expr(obj.conditional_encryption.when), F.lit(False)))
        for field in obj.conditional_encryption.columns:
            if col := present.get(field.lower()):
                df = df.withColumn(col, F.when(F.col(_COND_COL), enc(col)).otherwise(F.col(col)))
        df = df.drop(_COND_COL)

    if obj.history_encryption:
        match = F.col(present["field"]).isin(list(obj.history_encryption))
        for name in ("oldvalue", "newvalue"):
            col = present[name]
            df = df.withColumn(col, F.when(match, enc(col)).otherwise(F.col(col)))
    return df


def reencrypt_statement(obj: ObjectConfig, table: str, key: str, spec: CipherSpec) -> str:
    """UPDATE that encrypts existing values in every version once a field joins the PII config."""
    if key.startswith("history:"):
        field = key.removeprefix("history:").replace("'", "''")
        return (
            f"UPDATE {table} SET OldValue = {spec.encrypt_sql('OldValue')}, "
            f"NewValue = {spec.encrypt_sql('NewValue')} WHERE Field = '{field}'"
        )
    col = quote(key)
    where = f"{col} IS NOT NULL"
    if obj.conditional_encryption and key in obj.conditional_encryption.columns:
        where += f" AND coalesce(({obj.conditional_encryption.when}), false)"
    return f"UPDATE {table} SET {col} = {spec.encrypt_sql(col)} WHERE {where}"
