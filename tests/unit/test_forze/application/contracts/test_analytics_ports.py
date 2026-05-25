"""Tests for forze.application.contracts.analytics.ports (AnalyticsQueryPort)."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsQueryPort
from forze.application.contracts.analytics.types import AnalyticsRunOptions
from forze.application.contracts.base import CursorPage, page_from_limit_offset


class _Row(BaseModel):
    id: str


class _Alt(BaseModel):
    name: str


class _Params(BaseModel):
    pass


class _StubAnalytics:
    async def _offset(
        self,
        query_key: str,
        params: BaseModel,
        pagination,
        *,
        options: AnalyticsRunOptions | None,
        return_count: bool,
        return_type: type[BaseModel] | None,
        return_fields: Sequence[str] | None,
    ):
        _ = query_key, params, pagination, options
        if return_fields is not None:
            data: list[object] = [{"id": "1"}]
        elif return_type is not None:
            data = [return_type(name="x")]
        else:
            data = [_Row(id="1")]
        if return_count:
            return page_from_limit_offset(data, pagination, total=1)
        return page_from_limit_offset(data, pagination, total=None)

    async def run(
        self,
        query_key: str,
        params: BaseModel,
        pagination=None,
        *,
        options: AnalyticsRunOptions | None = None,
    ):
        return await self._offset(
            query_key,
            params,
            pagination,
            options=options,
            return_count=False,
            return_type=None,
            return_fields=None,
        )

    async def run_page(
        self,
        query_key: str,
        params: BaseModel,
        pagination=None,
        *,
        options: AnalyticsRunOptions | None = None,
    ):
        return await self._offset(
            query_key,
            params,
            pagination,
            options=options,
            return_count=True,
            return_type=None,
            return_fields=None,
        )

    async def run_chunked(
        self,
        query_key: str,
        params: BaseModel,
        pagination=None,
        *,
        options: AnalyticsRunOptions | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncIterator[Sequence[_Row]]:
        _ = fetch_batch_size
        page = await self.run(query_key, params, pagination, options=options)
        yield page.hits

    async def project_run(
        self,
        fields: Sequence[str],
        query_key: str,
        params: BaseModel,
        pagination=None,
        *,
        options: AnalyticsRunOptions | None = None,
    ):
        _ = fields
        return await self._offset(
            query_key,
            params,
            pagination,
            options=options,
            return_count=False,
            return_type=None,
            return_fields=("id",),
        )

    async def project_run_page(
        self,
        fields: Sequence[str],
        query_key: str,
        params: BaseModel,
        pagination=None,
        *,
        options: AnalyticsRunOptions | None = None,
    ):
        _ = fields
        return await self._offset(
            query_key,
            params,
            pagination,
            options=options,
            return_count=True,
            return_type=None,
            return_fields=("id",),
        )

    async def project_run_chunked(
        self,
        fields: Sequence[str],
        query_key: str,
        params: BaseModel,
        pagination=None,
        *,
        options: AnalyticsRunOptions | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncIterator[Sequence[dict]]:
        _ = fields, fetch_batch_size
        page = await self.project_run(fields, query_key, params, pagination, options=options)
        yield page.hits

    async def select_run(
        self,
        return_type: type[BaseModel],
        query_key: str,
        params: BaseModel,
        pagination=None,
        *,
        options: AnalyticsRunOptions | None = None,
    ):
        return await self._offset(
            query_key,
            params,
            pagination,
            options=options,
            return_count=False,
            return_type=return_type,
            return_fields=None,
        )

    async def select_run_page(
        self,
        return_type: type[BaseModel],
        query_key: str,
        params: BaseModel,
        pagination=None,
        *,
        options: AnalyticsRunOptions | None = None,
    ):
        return await self._offset(
            query_key,
            params,
            pagination,
            options=options,
            return_count=True,
            return_type=return_type,
            return_fields=None,
        )

    async def select_run_chunked(
        self,
        return_type: type[BaseModel],
        query_key: str,
        params: BaseModel,
        pagination=None,
        *,
        options: AnalyticsRunOptions | None = None,
        fetch_batch_size: int = 2000,
    ) -> AsyncIterator[Sequence[BaseModel]]:
        _ = fetch_batch_size
        page = await self.select_run(
            return_type,
            query_key,
            params,
            pagination,
            options=options,
        )
        yield page.hits

    async def _cursor(
        self,
        query_key: str,
        params: BaseModel,
        cursor,
        *,
        options: AnalyticsRunOptions | None,
        return_type: type[BaseModel] | None,
        return_fields: Sequence[str] | None,
    ):
        _ = query_key, params, cursor, options
        if return_fields is not None:
            return CursorPage(
                hits=[{"id": "1"}],
                next_cursor=None,
                prev_cursor=None,
                has_more=False,
            )
        if return_type is not None:
            return CursorPage(
                hits=[return_type(name="x")],
                next_cursor=None,
                prev_cursor=None,
                has_more=False,
            )
        return CursorPage(
            hits=[_Row(id="1")],
            next_cursor=None,
            prev_cursor=None,
            has_more=False,
        )

    async def run_cursor(
        self,
        query_key: str,
        params: BaseModel,
        cursor=None,
        *,
        options: AnalyticsRunOptions | None = None,
    ):
        return await self._cursor(
            query_key,
            params,
            cursor,
            options=options,
            return_type=None,
            return_fields=None,
        )

    async def project_run_cursor(
        self,
        fields: Sequence[str],
        query_key: str,
        params: BaseModel,
        cursor=None,
        *,
        options: AnalyticsRunOptions | None = None,
    ):
        _ = fields
        return await self._cursor(
            query_key,
            params,
            cursor,
            options=options,
            return_type=None,
            return_fields=("id",),
        )

    async def select_run_cursor(
        self,
        return_type: type[BaseModel],
        query_key: str,
        params: BaseModel,
        cursor=None,
        *,
        options: AnalyticsRunOptions | None = None,
    ):
        return await self._cursor(
            query_key,
            params,
            cursor,
            options=options,
            return_type=return_type,
            return_fields=None,
        )


def test_analytics_query_port_structural() -> None:
    stub: AnalyticsQueryPort[_Row] = _StubAnalytics()
    assert stub is not None


@pytest.mark.asyncio
async def test_run_page_default_projection() -> None:
    stub = _StubAnalytics()
    page = await stub.run_page("daily", _Params())
    assert page.count == 1
    assert page.hits[0].id == "1"


@pytest.mark.asyncio
async def test_select_run_page() -> None:
    stub = _StubAnalytics()
    page = await stub.select_run_page(_Alt, "daily", _Params())
    assert page.count == 1
    assert page.hits[0].name == "x"


@pytest.mark.asyncio
async def test_project_run_page() -> None:
    stub = _StubAnalytics()
    page = await stub.project_run_page(["id"], "daily", _Params())
    assert page.count == 1
    assert page.hits[0]["id"] == "1"


@pytest.mark.asyncio
async def test_run_chunked() -> None:
    stub = _StubAnalytics()
    chunks = [c async for c in stub.run_chunked("daily", _Params())]
    assert len(chunks) == 1
    assert chunks[0][0].id == "1"
