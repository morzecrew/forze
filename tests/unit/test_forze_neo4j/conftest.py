"""Pytest configuration for forze_neo4j unit tests."""

import pytest

# Skip the whole package when the neo4j optional dep is not installed.
pytest.importorskip("neo4j")
