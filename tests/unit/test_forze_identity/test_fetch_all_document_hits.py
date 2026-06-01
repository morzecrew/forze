"""Tests for :func:`~forze_identity.authz.services.grants.fetch_all_document_hits`."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel

from forze.application.contracts.base import CountlessPage
from forze.base.exceptions import CoreException
from forze_identity.authz.services.grants import fetch_all_document_hits


class _Row(BaseModel):
    id: str


@pytest.mark.asyncio
async def test_fetch_all_stops_at_max_pages() -> None:
    qry = AsyncMock()
    qry.find_many = AsyncMock(
        return_value=CountlessPage(hits=[_Row(id="1")], page=1, size=1)
    )

    with pytest.raises(CoreException, match="max_pages=2"):
        await fetch_all_document_hits(
            qry,
            filters={},
            page_size=1,
            max_pages=2,
        )

    assert qry.find_many.await_count == 2


@pytest.mark.asyncio
async def test_fetch_all_rejects_invalid_page_size() -> None:
    qry = AsyncMock()

    with pytest.raises(CoreException, match="page_size"):
        await fetch_all_document_hits(qry, filters={}, page_size=0)
