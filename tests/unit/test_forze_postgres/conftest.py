"""Pytest configuration for forze_postgres unit tests."""

import pytest

# Skip entire module if psycopg optional dep is not installed
pytest.importorskip("psycopg")
