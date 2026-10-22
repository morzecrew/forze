"""Firestore history reads: a missing snapshot is skipped, a failed query is raised.

Firestore's ``NotFound`` maps to the same ``not_found`` kind as a missing snapshot, so
``read_many`` cannot tell them apart by kind: a missing database read as an empty result.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

pytest.importorskip("google.cloud.firestore")

from google.api_core import exceptions as gax_exceptions

from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import Document
from forze_firestore.kernel.client.errors import exc_interceptor
from forze_firestore.kernel.gateways import FirestoreHistoryGateway
from tests.unit._gateway_codec_helpers import history_codecs_for

pytestmark = pytest.mark.unit


class _Doc(Document):
    name: str


_CODEC, _HISTORY_CODEC = history_codecs_for(_Doc)


class _Client:
    """A Firestore client whose queries find no row, or fail as the API fails."""

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error

    async def collection(self, name: str, *, database: str | None = None) -> str:
        return name

    @exc_interceptor.coroutine("firestore.query_stream")  # type: ignore[untyped-decorator]
    async def query_stream(self, coll: str, **_: Any) -> list[dict[str, Any]]:
        if self.error is not None:
            raise self.error

        return []


def _gateway(client: _Client) -> FirestoreHistoryGateway[_Doc]:
    return FirestoreHistoryGateway(
        relation=("(default)", "docs_history"),
        target_relation=("(default)", "docs"),
        client=client,  # type: ignore[arg-type]
        model_type=_Doc,
        codec=_CODEC,
        history_codec=_HISTORY_CODEC,
    )


async def test_read_many_skips_a_missing_snapshot() -> None:
    assert await _gateway(_Client()).read_many([uuid4()], [1]) == []


async def test_read_of_a_missing_snapshot_is_not_found() -> None:
    with pytest.raises(CoreException) as caught:
        await _gateway(_Client()).read(uuid4(), 1)

    assert caught.value.kind is ExceptionKind.NOT_FOUND


async def test_read_many_raises_when_the_query_fails_with_not_found() -> None:
    client = _Client(gax_exceptions.NotFound("database does not exist"))

    with pytest.raises(CoreException) as caught:
        await _gateway(client).read_many([uuid4()], [1])

    assert caught.value.kind is ExceptionKind.NOT_FOUND
