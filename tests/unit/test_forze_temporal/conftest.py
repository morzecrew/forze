"""Pytest configuration for forze_temporal unit tests."""

import pytest

# Skip entire module if temporalio optional dep is not installed
# WorkflowCommandPort is excluded per project directive
pytest.importorskip("temporalio")
