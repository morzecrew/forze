"""Stable fingerprints for deduplicating pooled resources."""

import hashlib
from urllib.parse import parse_qs, urlparse

# ----------------------- #


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


def connection_string_fingerprint(dsn: str) -> str:
    """Fingerprint a connection URL/DSN for LRU dedup (excludes password)."""

    parsed = urlparse(dsn)
    query = parse_qs(parsed.query)
    sslmode = query.get("sslmode", [""])[0]
    options = query.get("options", [""])[0]

    return stable_fingerprint(
        parsed.scheme or "",
        parsed.hostname or "",
        str(parsed.port or ""),
        parsed.path or "",
        parsed.username or "",
        sslmode,
        options,
    )
