"""Unit tests for the re-encrypt iterator's control flow (fake ports)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from forze.application.integrations.crypto import ReencryptReport, reencrypt_documents
from forze.base.exceptions import CoreException, ExceptionKind

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

    report = await reencrypt_documents(
        query,  # type: ignore[arg-type]
        command,  # type: ignore[arg-type]
        to_update=lambda d: {"email": d.email},
    )

    assert report == ReencryptReport(rewritten=2, skipped_missing=0)
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

    report = await reencrypt_documents(
        query,  # type: ignore[arg-type]
        command,  # type: ignore[arg-type]
        to_update=lambda d: d.email,
    )

    assert report.rewritten == 2
    assert [pk for pk, _, _ in command.calls] == ["a", "b"]


async def test_empty_collection_is_a_noop() -> None:
    command = _FakeCommand()

    report = await reencrypt_documents(
        _FakeQuery(),  # type: ignore[arg-type]
        command,  # type: ignore[arg-type]
        to_update=lambda d: d,
    )

    assert report == ReencryptReport(rewritten=0, skipped_missing=0)
    assert command.calls == []


async def test_skips_a_doc_deleted_before_the_write_back() -> None:
    """A row deleted between the stream read and the update is skipped, not fatal.

    On a live collection some streamed rows are always gone by the time the
    sweep writes them back; there is nothing left to re-encrypt, so the sweep
    counts the skip and keeps going instead of aborting the pass.
    """

    docs = [
        SimpleNamespace(id="a", rev=3, email="x"),
        SimpleNamespace(id="b", rev=5, email="y"),
    ]
    query = _FakeQuery(docs)

    class _MissingRowCommand(_FakeCommand):
        async def update(self, pk, rev, dto, **kw):  # type: ignore[no-untyped-def]
            if pk == "a":
                raise CoreException.not_found(f"Document not found: {pk}")

            return await super().update(pk, rev, dto, **kw)

    command = _MissingRowCommand()

    report = await reencrypt_documents(
        query,  # type: ignore[arg-type]
        command,  # type: ignore[arg-type]
        to_update=lambda d: {"email": d.email},
    )

    assert report == ReencryptReport(rewritten=1, skipped_missing=1)
    assert command.calls == [("b", 5, {"email": "y"})]


async def test_any_other_error_still_aborts_the_sweep() -> None:
    """Only a missing row is skipped; a revision conflict still propagates."""

    query = _FakeQuery([SimpleNamespace(id="a", rev=3, email="x")])

    class _ConflictingCommand(_FakeCommand):
        async def update(self, pk, rev, dto, **kw):  # type: ignore[no-untyped-def]
            raise CoreException.concurrency("Revision conflict")

    with pytest.raises(CoreException) as ei:
        await reencrypt_documents(
            query,  # type: ignore[arg-type]
            _ConflictingCommand(),  # type: ignore[arg-type]
            to_update=lambda d: {"email": d.email},
        )

    assert ei.value.kind is ExceptionKind.CONCURRENCY
