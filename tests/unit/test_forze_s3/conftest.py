"""Pytest configuration for forze_s3 unit tests."""

import pytest

# Skip entire module if aioboto3 optional dep is not installed
pytest.importorskip("aioboto3")
