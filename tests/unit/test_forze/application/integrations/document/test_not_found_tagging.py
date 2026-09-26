"""A document adapter's not-found names the spec it is about.

A non-disclosing posture collapses only errors that say which resource type they concern, so an
untagged not-found from any public method is one the posture cannot hide. Tagged per class, the
overrides of every backend's subclass included.
"""

from __future__ import annotations

import inspect
from types import SimpleNamespace
from typing import Any

import pytest

from forze.application.integrations.document import DocumentAdapter, DocumentNotFoundTagging
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_firestore.adapters.document import FirestoreDocumentAdapter
from forze_mock.adapters.document import MockDocumentAdapter
from forze_mongo.adapters.document import MongoDocumentAdapter
from forze_postgres.adapters.document import PostgresDocumentAdapter

# ----------------------- #


@pytest.mark.parametrize(
    "adapter",
    [
        DocumentAdapter,
        PostgresDocumentAdapter,
        MongoDocumentAdapter,
        FirestoreDocumentAdapter,
        MockDocumentAdapter,
    ],
    ids=lambda adapter: adapter.__name__,
)
def test_every_public_coroutine_is_tagged(adapter: type[Any]) -> None:
    coroutines = [
        name
        for name in dir(adapter)
        if not name.startswith("_") and inspect.iscoroutinefunction(getattr(adapter, name))
    ]

    assert "get" in coroutines
    assert [
        name
        for name in coroutines
        if not getattr(getattr(adapter, name), "__forze_tags_not_found__", False)
    ] == []


# ....................... #


class _Toy(DocumentNotFoundTagging):
    spec = SimpleNamespace(name="toys")  # type: ignore[assignment]  # only .name is read

    async def read(self, error: CoreException) -> None:
        raise error


class _Override(_Toy):
    async def read(self, error: CoreException) -> None:
        raise error


async def _raised(adapter: _Toy, error: CoreException) -> CoreException:
    with pytest.raises(CoreException) as caught:
        await adapter.read(error)

    return caught.value


async def test_an_override_in_a_subclass_is_tagged() -> None:
    error = await _raised(_Override(), exc.not_found("gone"))

    assert error.resource_type == "toys"


async def test_a_not_found_that_names_its_type_keeps_it() -> None:
    """The first adapter to see it tagged it; an outer one only passed it on."""

    error = await _raised(_Toy(), exc.not_found("gone", resource_type="parts"))

    assert error.resource_type == "parts"


@pytest.mark.parametrize("kind", [ExceptionKind.AUTHORIZATION, ExceptionKind.CONFLICT])
async def test_only_a_not_found_is_tagged(kind: ExceptionKind) -> None:
    error = await _raised(_Toy(), CoreException.of(kind, "no"))

    assert error.resource_type is None
