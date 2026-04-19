"""Tests for HTTP request :class:`~forze_fastapi.endpoints.http.mapping` mappers."""

import pytest
from pydantic import BaseModel

from forze.base.errors import CoreError
from forze.domain.models import BaseDTO
from forze_fastapi.endpoints.http.contracts import HttpRequestDTO
from forze_fastapi.endpoints.http.mapping import (
    BodyAsIsMapper,
    EmptyMapper,
    QueryAsIsBodyAssignMapper,
    QueryAsIsMapper,
)

# ----------------------- #


class _Q(BaseModel):
    n: int


class _B(BaseModel):
    s: str


class _Out(BaseModel):
    n: int
    body: dict


@pytest.mark.asyncio
async def test_empty_mapper_returns_base_dto() -> None:
    dto = HttpRequestDTO[None, None, None, None, None]()
    out = await EmptyMapper()(dto)
    assert isinstance(out, BaseDTO)


@pytest.mark.asyncio
async def test_query_as_is_mapper_success() -> None:
    m = QueryAsIsMapper(out=_Q)
    req = HttpRequestDTO(
        query=_Q(n=42),
        path=None,
        header=None,
        cookie=None,
        body=None,
    )
    assert (await m(req)).n == 42


@pytest.mark.asyncio
async def test_query_as_is_mapper_requires_query() -> None:
    m = QueryAsIsMapper(out=_Q)
    with pytest.raises(CoreError, match="Query is required"):
        await m(HttpRequestDTO())


@pytest.mark.asyncio
async def test_body_as_is_mapper_success() -> None:
    m = BodyAsIsMapper(out=_B)
    req = HttpRequestDTO(
        query=None,
        path=None,
        header=None,
        cookie=None,
        body=_B(s="hi"),
    )
    assert (await m(req)).s == "hi"


@pytest.mark.asyncio
async def test_body_as_is_mapper_requires_body() -> None:
    m = BodyAsIsMapper(out=_B)
    with pytest.raises(CoreError, match="Body is required"):
        await m(HttpRequestDTO())


def test_query_body_assign_rejects_unknown_body_key() -> None:
    with pytest.raises(CoreError, match="Body key"):
        QueryAsIsBodyAssignMapper(out=_Out, body_key="missing")


@pytest.mark.asyncio
async def test_query_body_assign_merges_query_and_body() -> None:
    m = QueryAsIsBodyAssignMapper(out=_Out, body_key="body")
    req = HttpRequestDTO(
        query=_Q(n=7),
        path=None,
        header=None,
        cookie=None,
        body=_B(s="x"),
    )
    out = await m(req)
    assert out.n == 7
    assert out.body == {"s": "x"}


@pytest.mark.asyncio
async def test_query_body_assign_requires_both() -> None:
    m = QueryAsIsBodyAssignMapper(out=_Out, body_key="body")
    with pytest.raises(CoreError, match="Query and body are required"):
        await m(
            HttpRequestDTO(
                query=_Q(n=1),
                path=None,
                header=None,
                cookie=None,
                body=None,
            )
        )
