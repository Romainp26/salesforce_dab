from __future__ import annotations

import base64
import csv
import shutil
from pathlib import Path

import pytest

from sf_ingest.config import OPS_SCHEMA, SCD2_SCHEMA, VIEW_SCHEMA, ObjectConfig, PiiSettings
from sf_ingest.encrypt import CipherSpec
from sf_ingest.metadata import FieldInfo

ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config"

# The current format: random key and IV, stored base64 (B-13). Test values only.
KEY = bytes(range(32))
IV = bytes(range(16, 32))
KEY_B64 = base64.b64encode(KEY).decode()
IV_B64 = base64.b64encode(IV).decode()
# The legacy format of backup_salesforce (Synapse, salesforce_dab): UTF-8 text key, hex IV.
LEGACY_KEY_TEXT = "k3y-for-tests-only-0123456789abc"
LEGACY_IV_HEX = "0f0e0d0c0b0a09080706050403020100"
CATALOG = "spark_catalog"
ARCHIVAL = ("005ARCHIVE000001AAA",)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    tmp = tmp_path_factory.mktemp("spark")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("sf_ingest_tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(tmp / "warehouse"))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={tmp / 'derby'}")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        # As on the job cluster and in tasks.main: base64 without line breaks.
        .config("spark.sql.chunkBase64String.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def catalog(spark):
    """A clean set of schemas in the local catalog, with the ops tables created."""
    from sf_ingest import ops

    for schema in (SCD2_SCHEMA, VIEW_SCHEMA, OPS_SCHEMA):
        spark.sql(f"DROP SCHEMA IF EXISTS `{CATALOG}`.`{schema}` CASCADE")
    ops.ensure_ops_tables(spark, CATALOG)
    return CATALOG


PII = PiiSettings(
    key_version=1,
    key_encoding="base64",
    iv_encoding="base64",
    iv_prefixed=True,
    base64_line_breaks=False,
    aes_key_secret="k",
    aes_iv_secret="iv",
)


@pytest.fixture
def spec() -> CipherSpec:
    return CipherSpec.from_values(f"'{KEY_B64}'", f"'{IV_B64}'", PII)


@pytest.fixture
def legacy_spec() -> CipherSpec:
    """How the seed reads backup_salesforce ciphertext; the tests stand in literals for secret()."""
    return CipherSpec(f"encode('{LEGACY_KEY_TEXT}', 'UTF-8')", f"unhex('{LEGACY_IV_HEX}')", iv_prefixed=True)


ACCOUNT_FIELDS = [
    FieldInfo("Id", "id"),
    FieldInfo("Name", "string"),
    FieldInfo("Type", "picklist"),
    FieldInfo("Phone__c", "phone"),
    FieldInfo("Amount__c", "currency", precision=18, scale=2),
    FieldInfo("Visits__c", "int"),
    FieldInfo("IsDeleted", "boolean"),
    FieldInfo("LastModifiedById", "reference"),
    FieldInfo("CloseDate__c", "date"),
    FieldInfo("LastViewedDate", "datetime"),
    FieldInfo("CreatedDate", "datetime"),
    FieldInfo("SystemModstamp", "datetime"),
]
ACCOUNT_HEADER = [f.name for f in ACCOUNT_FIELDS]


def account_config(**overrides) -> ObjectConfig:
    from sf_ingest.config import ConditionalEncryption

    base = dict(
        name="Account",
        encrypted_fields=("Phone__c",),
        conditional_encryption=ConditionalEncryption(
            columns=("Name",), when="Type IS NULL OR lower(Type) IN ('customer', 'prospect')"
        ),
        hash_exclude=("SystemModstamp", "LastViewedDate", "LastReferencedDate"),
    )
    base.update(overrides)
    return ObjectConfig(**base)


def account_row(
    id_,
    name="Acme",
    type_="Customer",
    phone="0812345678",
    amount="10.50",
    visits="3",
    deleted="false",
    modified_by="005USER00000001AAA",
    close="2024-05-01",
    viewed="",
    created="2024-05-01T01:00:00.000Z",
    modstamp="2024-05-01T02:00:00.000Z",
):
    return [id_, name, type_, phone, amount, visits, deleted, modified_by, close, viewed, created, modstamp]


def write_page(directory: Path, n: int, header: list[str], rows: list[list[str]]) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"page_{n}.csv"
    with path.open("w", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
    return path


@pytest.fixture
def landing(tmp_path):
    def make(name: str, rows: list[list[str]], header=ACCOUNT_HEADER) -> Path:
        directory = tmp_path / name
        shutil.rmtree(directory, ignore_errors=True)
        write_page(directory, 1, header, rows)
        return directory

    return make
