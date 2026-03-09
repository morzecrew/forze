"""Pytest configuration for forze_rabbitmq unit tests."""

import pytest

# Skip entire module if aio-pika optional dep is not installed
pytest.importorskip("aio_pika")
