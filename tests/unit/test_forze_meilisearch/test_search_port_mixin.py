"""Cursor methods are not implemented on Meilisearch search adapters."""

import pytest

from forze.base.exceptions import CoreException
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze_meilisearch.adapters.search._port import MeilisearchSearchPortMixin
from forze_meilisearch.execution.deps.configs import MeilisearchSearchConfig


class _M(BaseModel):
    id: str
    title: str = ""


class _Adapter(MeilisearchSearchPortMixin[_M]):
    spec = SearchSpec(name="s", model_type=_M, fields=["title"])
    model_type = _M
    config: MeilisearchSearchConfig = MeilisearchSearchConfig(index_uid="items")


@pytest.mark.asyncio
async def test_search_cursor_raises() -> None:
    adapter = _Adapter()

    with pytest.raises(CoreException):
        await adapter.search_cursor("q")


def test_capabilities_declare_estimated_total() -> None:
    """Meilisearch reports estimatedTotalHits, so page totals are flagged approximate."""

    assert _Adapter().search_capabilities.exact_total_count is False
