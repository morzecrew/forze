"""A Mongo route reaches the ``namespace`` tier only when every collection it names does.

The Postgres mirror is `tests/unit/test_forze_postgres/test_pg_deps_namespace_tier.py`.

# covers: MongoDepsModule.__attrs_post_init__
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pytest

from forze.base.exceptions import CoreException
from forze_mongo.execution.deps import MongoDepsModule
from forze_mongo.execution.deps.configs import (
    MongoAtlasEngine,
    MongoCounterConfig,
    MongoDocumentConfig,
    MongoOutboxConfig,
    MongoReadOnlyDocumentConfig,
    MongoSearchConfig,
    MongoTextEngine,
)
from forze_mongo.kernel.client import MongoClient

# ----------------------- #


def _collection(name: str, *, static: bool) -> Any:
    if static:
        return ("app", name)

    return lambda tenant_id: (f"tenant_{tenant_id}", name)


def _document(static: str | None) -> dict[str, Any]:
    return {
        "rw_documents": {
            "orders": MongoDocumentConfig(
                read=_collection("orders_view", static=static == "read"),
                write=_collection("orders", static=static == "write"),
                history=_collection("orders_history", static=static == "history"),
            ),
        },
    }


def _search(static: str | None, *, engine: Any = None) -> dict[str, Any]:
    return {
        "searches": {
            "orders": MongoSearchConfig(
                read=_collection("orders", static=static == "read"),
                engine=engine or MongoAtlasEngine(index_name=lambda t: f"orders_{t}"),
            ),
        },
    }


def _counter(static: str | None) -> dict[str, Any]:
    return {
        "counters": {
            "orders": MongoCounterConfig(collection=_collection("counters", static=bool(static))),
        },
    }


def _outbox(static: str | None, *, tenant_aware: bool = True) -> dict[str, Any]:
    return {
        "outboxes": {
            "orders": MongoOutboxConfig(
                collection=_collection("outbox", static=bool(static)),
                tenant_aware=tenant_aware,
            ),
        },
    }


_ROUTES: dict[str, tuple[Callable[[str | None], dict[str, Any]], tuple[str, ...]]] = {
    "document": (_document, ("read", "write", "history")),
    "search": (_search, ("read",)),
    "counter": (_counter, ("collection",)),
    "outbox": (_outbox, ("collection",)),
}


def _module(*, floor: str | None, **routes: Any) -> MongoDepsModule:
    # A compliant sibling, so a refusal is the route's and never an empty module's.
    return MongoDepsModule(
        client=MongoClient(),
        required_tenant_isolation=floor,  # type: ignore[arg-type]
        ro_documents={"baseline": MongoReadOnlyDocumentConfig(read=_collection("b", static=False))},
        **routes,
    )


# ....................... #


@pytest.mark.parametrize("kind", list(_ROUTES))
def test_a_route_resolving_every_collection_per_tenant_meets_a_namespace_floor(kind: str) -> None:
    build, _ = _ROUTES[kind]

    assert _module(floor="namespace", **build(None)) is not None


@pytest.mark.parametrize(
    ("build", "field"),
    [
        pytest.param(build, field, id=f"{kind}-{field}")
        for kind, (build, fields) in _ROUTES.items()
        for field in fields
    ],
)
def test_one_static_collection_drops_the_route_below_a_namespace_floor(
    build: Callable[[str | None], dict[str, Any]],
    field: str,
) -> None:
    with pytest.raises(CoreException, match="mongo_tenancy_validation_failed"):
        _module(floor="namespace", **build(field))

    # The control: with no floor declared the same wiring stands, so the refusal is the floor's.
    assert _module(floor=None, **build(field)) is not None


@pytest.mark.parametrize(
    "engine",
    [MongoTextEngine(), MongoAtlasEngine(index_name="default")],
    ids=["text", "atlas-static-index-name"],
)
def test_a_search_on_a_per_tenant_collection_meets_a_namespace_floor(engine: Any) -> None:
    # An index name, where the engine has one, is scoped to the collection it searches.
    assert _module(floor="namespace", **_search(None, engine=engine)) is not None


def test_an_outbox_resolving_its_collection_without_the_tenant_is_not_namespace() -> None:
    # Not tenant-aware, the outbox calls its resolver with no tenant: one collection for all.
    with pytest.raises(CoreException, match="mongo_tenancy_validation_failed"):
        _module(floor="namespace", **_outbox(None, tenant_aware=False))


def test_a_tenant_aware_outbox_is_not_advised_to_drop_the_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Dropping it would have the outbox call its resolver with no tenant.
    import forze_mongo.execution.deps.module as mongo_module

    log = MagicMock()
    monkeypatch.setattr(mongo_module, "logger", log)

    _module(floor=None, **_outbox(None, tenant_aware=True))

    advice = [str(call) for call in log.warning.call_args_list]
    assert not [line for line in advice if "outbox" in line], advice
