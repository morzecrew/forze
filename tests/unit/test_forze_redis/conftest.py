"""Pytest configuration for forze_redis unit tests."""

import pytest

# Skip entire module if redis optional dep is not installed
pytest.importorskip("redis")
