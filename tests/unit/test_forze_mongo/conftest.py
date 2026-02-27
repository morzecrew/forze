"""Pytest configuration for forze_mongo unit tests."""

import pytest

# Skip entire module if pymongo optional dep is not installed
pytest.importorskip("pymongo")
