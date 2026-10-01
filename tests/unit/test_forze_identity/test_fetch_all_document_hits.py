"""Tests for :func:`~forze_identity.authz.services.grants.fetch_all_document_hits`."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from forze.base.exceptions import CoreException
from forze_identity.authz.services.grants import fetch_all_document_hits


class _Row(BaseModel):
    id: str


class _Endless:
    """A query port whose keyset stream never runs out, and which records being closed."""

    def __init__(self) -> None:
        self.batches = 0
        self.closed = False

    async def find_stream(self, *, filters: Any, chunk_size: int) -> Any:
        try:
            while True:
                self.batches += 1
                yield [_Row(id=str(self.batches))] * chunk_size
        finally:
            self.closed = True


@pytest.mark.asyncio
async def test_fetch_all_stops_at_max_pages_and_closes_the_stream() -> None:
    qry = _Endless()

    with pytest.raises(CoreException, match="max_pages=2"):
        await fetch_all_document_hits(qry, filters={}, page_size=1, max_pages=2)  # type: ignore[arg-type]

    # The refusing batch is the third; the stream is closed then, not left to the collector.
    assert (qry.batches, qry.closed) == (3, True)


@pytest.mark.asyncio
async def test_fetch_all_rejects_invalid_page_size() -> None:
    with pytest.raises(CoreException, match="page_size"):
        await fetch_all_document_hits(_Endless(), filters={}, page_size=0)  # type: ignore[arg-type]
