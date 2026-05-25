"""Pytest configuration for forze_clickhouse unit tests."""

from __future__ import annotations

import pytest

pytest.importorskip("clickhouse_connect")
