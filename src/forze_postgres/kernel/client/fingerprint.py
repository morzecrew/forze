"""Connection fingerprinting for routed Postgres pool deduplication."""

from forze.base.primitives.fingerprint import connection_string_fingerprint

# ----------------------- #


def postgres_connection_fingerprint(dsn: str) -> str:
    """Fingerprint a Postgres DSN for LRU dedup (excludes password)."""

    return connection_string_fingerprint(dsn)
