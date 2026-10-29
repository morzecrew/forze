"""``FirestoreCounterAdapter`` reports what it supports (no emulator)."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from forze.application.contracts.counter import counter_capabilities
from forze_firestore.adapters.counter import FirestoreCounterAdapter
from forze_firestore.execution.deps.configs import FirestoreCounterConfig

pytestmark = pytest.mark.unit

# ----------------------- #


def test_reports_expiry() -> None:
    counter = FirestoreCounterAdapter(
        client=Mock(),
        config=FirestoreCounterConfig(collection=("db", "counters")),
        route="orders",
    )

    assert counter_capabilities(counter).expiry is True
