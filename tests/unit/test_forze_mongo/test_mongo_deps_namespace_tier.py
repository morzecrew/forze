"""A Mongo route reaches the ``namespace`` tier only when every collection it names does.

The Postgres mirror is `tests/unit/test_forze_postgres/test_pg_deps_namespace_tier.py`.

# covers: MongoDepsModule.__attrs_post_init__
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from forze.base.exceptions import CoreException
from forze_mongo.execution.deps import MongoDepsModule
from forze_mongo.execution.deps.configs import (
    MongoAtlasEngine,
    MongoDocumentConfig,
    MongoSearchConfig,
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


def _search(static: str | None) -> dict[str, Any]:
    return {
        "searches": {
            "orders": MongoSearchConfig(
                read=_collection("orders", static=static == "read"),
                engine=MongoAtlasEngine(
                    index_name="default"
                    if static == "index_name"
                    else lambda tenant_id: f"orders_{tenant_id}",
                ),
            ),
        },
    }


_ROUTES: dict[str, tuple[Callable[[str | None], dict[str, Any]], tuple[str, ...]]] = {
    "document": (_document, ("read", "write", "history")),
    "search": (_search, ("index_name", "read")),
}


def _module(*, floor: str | None, **routes: Any) -> MongoDepsModule:
    return MongoDepsModule(
        client=MongoClient(),
        required_tenant_isolation=floor,  # type: ignore[arg-type]
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
