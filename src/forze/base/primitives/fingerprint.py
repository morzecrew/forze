"""Stable fingerprints for deduplicating pooled resources."""

import hashlib
import hmac
from urllib.parse import parse_qs, urlparse

from pydantic import SecretStr

# ----------------------- #

_POOL_DEDUP_DOMAIN = b"forze.pool-dedup.v1"

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
    """Return a one-way tag for secret material in LRU pool dedup (not credential storage).

    Uses HMAC-SHA256 with a fixed domain key. Returns ``""`` for ``None`` or empty values.
    """

    if value is None:
        return ""

    raw = value.get_secret_value() if isinstance(value, SecretStr) else value

    if not raw:
        return ""

    return hmac.new(
        _POOL_DEDUP_DOMAIN,
        raw.encode("utf-8"),  # codeql[py/weak-sensitive-data-hashing]
        hashlib.sha256,
    ).hexdigest()


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

    return stable_fingerprint(
        parsed.scheme or "",
        parsed.hostname or "",
        str(parsed.port or ""),
        parsed.path or "",
        parsed.username or "",
        sslmode,
        options,
        password_tag,
        query_canonical,
    )
