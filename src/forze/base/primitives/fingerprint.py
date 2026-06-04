"""Stable fingerprints for deduplicating pooled resources."""

import binascii
import hashlib
from collections.abc import Sequence
from urllib.parse import parse_qs, urlparse

from pydantic import SecretStr

# ----------------------- #

_POOL_DEDUP_DOMAIN = b"forze.pool-dedup.v1"

# scrypt cost parameters for secret dedup tags — intentionally minimal. These tags are
# in-memory LRU pool-dedup keys: never persisted, never transmitted, so the offline
# brute-force threat that warrants a high work factor does not apply. scrypt (a
# recognized one-way KDF) is used only so secret material never reaches a fast hash.
# Do NOT reuse these parameters for credential storage.
_SECRET_DEDUP_SCRYPT_N = 2
_SECRET_DEDUP_SCRYPT_R = 8
_SECRET_DEDUP_SCRYPT_P = 1

# ....................... #


def stable_fingerprint(*parts: str | bytes) -> str:
    """Return a SHA-256 hex digest of normalized ``parts`` (for cache keys only)."""

    normalized: list[bytes] = []

    for part in parts:
        if isinstance(part, str):
            normalized.append(part.encode("utf-8"))

        else:
            normalized.append(part)

    return hashlib.sha256(b"\x1f".join(normalized)).hexdigest()


# ....................... #


def secret_dedup_fingerprint(value: str | SecretStr | None) -> str:
    """Return a deterministic one-way tag for secret material in LRU pool dedup.

    Uses ``scrypt`` with a fixed domain salt and minimal cost parameters: a recognized
    one-way KDF keeps secret material out of fast hashes, while the low work factor
    suits an in-memory dedup key (see the cost-parameter note above). Tags stay stable
    for the same secret. Returns ``""`` for ``None`` or empty values.
    """

    if value is None:
        return ""

    raw = value.get_secret_value() if isinstance(value, SecretStr) else value

    if not raw:
        return ""

    derived = hashlib.scrypt(
        raw.encode("utf-8"),
        salt=_POOL_DEDUP_DOMAIN,
        n=_SECRET_DEDUP_SCRYPT_N,
        r=_SECRET_DEDUP_SCRYPT_R,
        p=_SECRET_DEDUP_SCRYPT_P,
    )
    return binascii.hexlify(derived).decode("ascii")


# ....................... #


def combine_fingerprint(base: str, *secret_tags: str) -> str:
    """Append already-hashed secret dedup tags to *base* without re-hashing them.

    *base* is a :func:`stable_fingerprint` of non-secret fields; each tag comes from
    :func:`secret_dedup_fingerprint` (a one-way KDF digest). Joining the digests
    (instead of re-hashing) keeps the combined key unique while ensuring secret-derived
    data never passes through the fast cache hash again. Empty tags are ignored.
    """

    tags = [tag for tag in secret_tags if tag]

    if not tags:
        return base

    return "\x1f".join((base, *tags))


# ....................... #


def build_routing_fingerprint(
    *,
    public: Sequence[str],
    secret: Sequence[str | SecretStr | None] = (),
) -> str:
    """Build an LRU pool dedup key from non-secret fields plus secret material.

    *public* fields are hashed together via :func:`stable_fingerprint`; each *secret*
    is reduced to a one-way tag via :func:`secret_dedup_fingerprint` and concatenated on
    via :func:`combine_fingerprint`, so secret material never reaches a fast hash. Routed
    clients should build ``credential_fingerprint`` with this rather than hand-assembling
    hashes: declaring **every** credential field (including secrets) keeps rotation
    detection correct, since a changed secret then changes the key.
    """

    return combine_fingerprint(
        stable_fingerprint(*public),
        *(secret_dedup_fingerprint(item) for item in secret),
    )


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
    query_canonical = "&".join(f"{key}={query[key][0]}" for key in sorted(query))

    return build_routing_fingerprint(
        public=[
            parsed.scheme or "",
            parsed.hostname or "",
            str(parsed.port or ""),
            parsed.path or "",
            parsed.username or "",
            sslmode,
            options,
            query_canonical,
        ],
        secret=[parsed.password],
    )
