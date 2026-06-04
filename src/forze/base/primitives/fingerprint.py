"""Stable fingerprints for deduplicating pooled resources."""

import binascii
import hashlib
import hmac
from urllib.parse import parse_qs, urlparse

from pydantic import SecretStr

# ----------------------- #

_POOL_DEDUP_DOMAIN = b"forze.pool-dedup.v1"
_SECRET_DEDUP_PBKDF2_ITERATIONS = 200_000

# ....................... #


def stable_fingerprint(*parts: str | bytes) -> str:
    """Return a SHA-256 hex digest of normalized ``parts`` (for cache keys only)."""

    normalized: list[bytes] = []

    for part in parts:
        if isinstance(part, str):
            normalized.append(part.encode("utf-8"))

        else:
            normalized.append(part)

    return hashlib.sha256(
        b"\x1f".join(normalized),  # codeql[py/weak-sensitive-data-hashing]
    ).hexdigest()


# ....................... #


def secret_dedup_fingerprint(value: str | SecretStr | None) -> str:
    """Return a deterministic one-way tag for secret material in LRU pool dedup.

    Uses PBKDF2-HMAC-SHA256 with a fixed domain salt to keep tags stable for dedup.
    Returns ``""`` for ``None`` or empty values.
    """

    if value is None:
        return ""

    raw = value.get_secret_value() if isinstance(value, SecretStr) else value

    if not raw:
        return ""

    derived = hashlib.pbkdf2_hmac(
        "sha256",
        raw.encode("utf-8"),
        _POOL_DEDUP_DOMAIN,
        _SECRET_DEDUP_PBKDF2_ITERATIONS,
    )
    return binascii.hexlify(derived).decode("ascii")


# ....................... #


def gcp_credential_dedup_tag(
    *,
    service_file: str | None = None,
    service_account_json: str | None = None,
) -> str:
    """Return a dedup tag for GCP credential sources (never embeds raw JSON)."""

    if service_file is not None:
        return f"file:{service_file}"

    if service_account_json is not None:
        return f"inline:{secret_dedup_fingerprint(service_account_json)}"

    return "adc"


# ....................... #


def connection_string_fingerprint(dsn: str) -> str:
    """Fingerprint a connection URL/DSN for LRU dedup.

    Includes scheme, host, port, path, username, sorted query parameters, and a one-way
    password tag when a password is present in the URI (the raw password is never stored).
    """

    parsed = urlparse(dsn)
    query = parse_qs(parsed.query)
    sslmode = query.get("sslmode", [""])[0]
    options = query.get("options", [""])[0]
    password_tag = secret_dedup_fingerprint(parsed.password) if parsed.password else ""
    query_canonical = "&".join(f"{key}={query[key][0]}" for key in sorted(query))

    base_fp = stable_fingerprint(
        parsed.scheme or "",
        parsed.hostname or "",
        str(parsed.port or ""),
        parsed.path or "",
        parsed.username or "",
        sslmode,
        options,
        query_canonical,
    )

    if not password_tag:
        return base_fp

    return hmac.new(
        _POOL_DEDUP_DOMAIN,
        f"{base_fp}\x1f{password_tag}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
