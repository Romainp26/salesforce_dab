import base64
from datetime import UTC, datetime, timedelta

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509.oid import NameOID

from sf_ingest.auth import load_key_material


def _key_and_cert(days: int = 365):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "sf-ingest")])
    now = datetime.now(UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(1)
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=days))
        .sign(key, hashes.SHA256())
    )
    return key, cert


def test_pem_key_with_certificate():
    key, cert = _key_and_cert(days=20)
    pem = (
        key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()
        ).decode()
        + cert.public_bytes(serialization.Encoding.PEM).decode()
    )
    material = load_key_material(pem)
    assert material.private_key_pem.startswith("-----BEGIN PRIVATE KEY-----")
    assert material.cert_not_after == cert.not_valid_after_utc
    assert material.cert_subject == "CN=sf-ingest"


def test_pem_key_without_certificate_has_no_expiry():
    key, _ = _key_and_cert()
    pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    assert load_key_material(pem).cert_not_after is None


def test_base64_pfx_with_password_converts_to_pem():
    key, cert = _key_and_cert()
    pfx = pkcs12.serialize_key_and_certificates(
        b"sf", key, cert, None, serialization.BestAvailableEncryption(b"secret")
    )
    material = load_key_material(base64.b64encode(pfx).decode(), "secret")
    loaded = serialization.load_pem_private_key(material.private_key_pem.encode(), password=None)
    assert loaded.private_numbers() == key.private_numbers()
    assert material.cert_not_after == cert.not_valid_after_utc
