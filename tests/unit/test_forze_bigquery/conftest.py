"""Pytest configuration for forze_bigquery unit tests."""

import pytest

pytest.importorskip("gcloud.aio.bigquery")
