"""Stable fingerprints for deduplicating pooled resources."""

import binascii
import hashlib
from collections.abc import Sequence
from typing import Any
from urllib.parse import parse_qs, urlparse

import orjson
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


def stable_json_bytes(payload: Any) -> bytes:
    """Return canonical, key-sorted UTF-8 JSON bytes for fingerprinting.

    Keys are sorted at every level (``OPT_SORT_KEYS``) for determinism; values
    orjson cannot serialize natively fall back to ``str``. For cache/dedup
    fingerprints only — the exact byte layout is not a stable wire format across
    orjson versions/options (notably ``datetime`` is serialized natively, so a
    payload's bytes differ from a ``str``-coerced encoding).
    """

    return orjson.dumps(payload, option=orjson.OPT_SORT_KEYS, default=str)


# ....................... #


def stable_payload_fingerprint(payload: Any, *, prefix: str = "sha256") -> str:
    """SHA-256 fingerprint of a payload's canonical JSON (for cache/dedup keys).

    Returns ``"{prefix}:{hexdigest}"`` (or the bare digest when ``prefix`` is
    empty). Uses :func:`stable_json_bytes` for deterministic, key-sorted input.
    """

    digest = hashlib.sha256(stable_json_bytes(payload)).hexdigest()

    return f"{prefix}:{digest}" if prefix else digest


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

    return "\x1f".join((base, *tags)) if tags else base


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

    Includes scheme, the full host list, path, username, sorted query parameters,
    and a one-way password tag when a password is present in the URI (the raw
    password is never stored).

    The host list is taken from the raw netloc (after stripping any
    ``user:pass@`` userinfo) rather than ``parsed.hostname``/``parsed.port``:
    multi-host DSNs (Mongo replica sets, Redis Sentinel, AMQP clusters) carry a
    comma-separated ``host:port,host:port`` authority that ``parsed.port``
    cannot parse (it raises :class:`ValueError`) and that ``parsed.hostname``
    truncates to the first host (so distinct host sets would collide).
    """

    parsed = urlparse(dsn)
    query = parse_qs(parsed.query)
    # All query parameters (sslmode, options, …) are canonicalized here; no
    # need to also list any individually below.
    query_canonical = "&".join(f"{key}={query[key][0]}" for key in sorted(query))

    # Authority after any userinfo; ``rpartition`` matches how urllib splits the
    # host portion. Lowercased because hostnames are case-insensitive.
    hosts = parsed.netloc.rpartition("@")[2].lower()

    return build_routing_fingerprint(
        public=[
            parsed.scheme or "",
            hosts,
            parsed.path or "",
            parsed.username or "",
            query_canonical,
        ],
        secret=[parsed.password],
    )
