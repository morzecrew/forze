"""A Firestore route reaches the ``namespace`` tier only when every collection it names does.

The Postgres mirror is `tests/unit/test_forze_postgres/test_pg_deps_namespace_tier.py`.

# covers: FirestoreDepsModule.__attrs_post_init__
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from forze.base.exceptions import CoreException
from forze_firestore.execution.deps.configs import (
    FirestoreCounterConfig,
    FirestoreDocumentConfig,
)
from forze_firestore.execution.deps.module import FirestoreDepsModule

# ----------------------- #


def _collection(name: str, *, static: bool) -> Any:
    if static:
        return ("(default)", name)

    return lambda tenant_id: ("(default)", f"tenant_{tenant_id}_{name}")


def _module(*, floor: str | None, static: str | None) -> FirestoreDepsModule:
    return FirestoreDepsModule(
        client=MagicMock(),
        required_tenant_isolation=floor,  # type: ignore[arg-type]
        rw_documents={
            "orders": FirestoreDocumentConfig(
                read=_collection("orders_view", static=static == "read"),
                write=_collection("orders", static=static == "write"),
                history=_collection("orders_history", static=static == "history"),
            ),
        },
    )


# ....................... #


def test_a_document_resolving_every_collection_per_tenant_meets_a_namespace_floor() -> None:
    assert _module(floor="namespace", static=None) is not None


@pytest.mark.parametrize("field", ["read", "write", "history"])
def test_one_static_collection_drops_the_document_below_a_namespace_floor(field: str) -> None:
    with pytest.raises(CoreException, match="firestore_tenancy_validation_failed"):
        _module(floor="namespace", static=field)

    # The control: with no floor declared the same wiring stands, so the refusal is the floor's.
    assert _module(floor=None, static=field) is not None


@pytest.mark.parametrize("static", [False, True], ids=["per-tenant", "static"])
def test_a_counter_meets_a_namespace_floor_only_on_a_per_tenant_collection(static: bool) -> None:
    def module(*, floor: str | None) -> FirestoreDepsModule:
        return FirestoreDepsModule(
            client=MagicMock(),
            required_tenant_isolation=floor,  # type: ignore[arg-type]
            counters={
                "orders": FirestoreCounterConfig(collection=_collection("ctr", static=static))
            },
        )

    if not static:
        assert module(floor="namespace") is not None
        return

    with pytest.raises(CoreException, match="firestore_tenancy_validation_failed"):
        module(floor="namespace")

    assert module(floor=None) is not None
