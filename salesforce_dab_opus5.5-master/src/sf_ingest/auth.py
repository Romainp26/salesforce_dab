"""OAuth 2.0 JWT bearer authentication against a Salesforce External Client App (D-05, D-21)."""

from __future__ import annotations

import base64
import logging
import re
from dataclasses import dataclass
from datetime import datetime

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import pkcs12
from simple_salesforce import Salesforce

log = logging.getLogger(__name__)

_CERT_PEM = re.compile(r"-----BEGIN CERTIFICATE-----.+?-----END CERTIFICATE-----", re.S)


@dataclass(frozen=True)
class KeyMaterial:
    private_key_pem: str
    cert_not_after: datetime | None
    cert_subject: str | None


def load_key_material(secret: str, password: str | None = None) -> KeyMaterial:
    """Accept a PEM private key (optionally followed by its certificate) or a base64 PFX."""
    secret = secret.strip()
    if "-----BEGIN" in secret:
        cert = None
        if m := _CERT_PEM.search(secret):
            cert = x509.load_pem_x509_certificate(m.group(0).encode())
        key = serialization.load_pem_private_key(secret.encode(), password=password.encode() if password else None)
    else:
        key, cert, _ = pkcs12.load_key_and_certificates(
            base64.b64decode(secret), password.encode() if password else None
        )
        if key is None:
            raise ValueError("The PFX holds no private key")
    pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    return KeyMaterial(
        private_key_pem=pem,
        cert_not_after=cert.not_valid_after_utc if cert else None,
        cert_subject=cert.subject.rfc4514_string() if cert else None,
    )


class SalesforceSession:
    """Holds one authenticated simple-salesforce client and re-authenticates on demand.

    The Bulk API client reuses the same access token (D-21).
    """

    def __init__(self, username: str, consumer_key: str, private_key_pem: str, domain: str, api_version: str):
        self._username = username
        self._consumer_key = consumer_key
        self._private_key_pem = private_key_pem
        self._domain = domain
        self.api_version = api_version
        self._sf: Salesforce | None = None

    @property
    def sf(self) -> Salesforce:
        if self._sf is None:
            self.refresh()
        return self._sf

    def refresh(self) -> None:
        log.info("Authenticating to Salesforce domain %s with JWT bearer flow", self._domain)
        self._sf = Salesforce(
            username=self._username,
            consumer_key=self._consumer_key,
            privatekey=self._private_key_pem,
            domain=self._domain,
            version=self.api_version,
        )

    @property
    def instance_url(self) -> str:
        return f"https://{self.sf.sf_instance}"

    @property
    def access_token(self) -> str:
        return self.sf.session_id
