"""Unit tests for the re-encrypt iterator's control flow (fake ports)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from forze.application.integrations.crypto import reencrypt_documents

# ----------------------- #


class _FakeQuery:
    def __init__(self, *batches: list[Any]) -> None:
        self._batches = batches
        self.stream_calls = 0

    async def find_stream(self, filters=None, *, sorts=None, chunk_size=500):  # type: ignore[no-untyped-def]
        self.stream_calls += 1
        for batch in self._batches:
            yield batch


class _FakeCommand:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, int, Any]] = []

    async def update(self, pk, rev, dto, **kw):  # type: ignore[no-untyped-def]
        self.calls.append((pk, rev, dto))
        return None


# ....................... #


async def test_reencrypts_every_doc_passing_id_rev_and_update() -> None:
    docs = [
        SimpleNamespace(id="a", rev=3, email="x"),
        SimpleNamespace(id="b", rev=5, email="y"),
    ]
    query = _FakeQuery(docs)
    command = _FakeCommand()

    count = await reencrypt_documents(
        query,  # type: ignore[arg-type]
        command,  # type: ignore[arg-type]
        to_update=lambda d: {"email": d.email},
    )

    assert count == 2
    assert command.calls == [
        ("a", 3, {"email": "x"}),
        ("b", 5, {"email": "y"}),
    ]


async def test_streams_across_multiple_batches() -> None:
    query = _FakeQuery(
        [SimpleNamespace(id="a", rev=1, email="x")],
        [SimpleNamespace(id="b", rev=1, email="y")],
    )
    command = _FakeCommand()

    count = await reencrypt_documents(
        query,  # type: ignore[arg-type]
        command,  # type: ignore[arg-type]
        to_update=lambda d: d.email,
    )

    assert count == 2
    assert [pk for pk, _, _ in command.calls] == ["a", "b"]


async def test_empty_collection_is_a_noop() -> None:
    command = _FakeCommand()

    count = await reencrypt_documents(
        _FakeQuery(),  # type: ignore[arg-type]
        command,  # type: ignore[arg-type]
        to_update=lambda d: d,
    )

    assert count == 0
    assert command.calls == []
