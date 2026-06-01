"""Tests for :mod:`forze_postgres.kernel.client.fingerprint`."""

from forze.base.primitives.fingerprint import connection_string_fingerprint
from forze_postgres.kernel.client.fingerprint import postgres_connection_fingerprint


def test_postgres_connection_fingerprint_matches_base_helper() -> None:
    dsn = "postgresql://user:secret@localhost:5432/appdb"
    assert postgres_connection_fingerprint(dsn) == connection_string_fingerprint(dsn)
