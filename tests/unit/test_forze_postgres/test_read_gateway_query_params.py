"""Query parameters on the read gateway: set_config prelude in a tx + fail-closed guard."""

import asyncio
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from forze.base.exceptions import CoreException
from forze.domain.models import Document
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client import PostgresClient
from forze_postgres.kernel.gateways import PostgresReadGateway
from tests.unit._gateway_codec_helpers import codec_for

# ----------------------- #


class _Row(Document):
    title: str = "x"


class _Params(BaseModel):
    window: str = "2026-01-01"


class _OptionalParams(BaseModel):
    window: str | None = None
    region: str | None = None


def _client() -> MagicMock:
    client = MagicMock(spec=PostgresClient)
    client.fetch_all = AsyncMock(return_value=[])
    client.fetch_one = AsyncMock(return_value=None)
    client.execute = AsyncMock(return_value=None)
    client.gather_concurrency_semaphore = MagicMock(return_value=asyncio.Semaphore(8))

    @asynccontextmanager
    async def _tx() -> Any:
        yield None

    client.transaction = MagicMock(side_effect=lambda *a, **k: _tx())
    return client


def _intro() -> MagicMock:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(return_value={})
    intro.cache_partition_key = None
    return intro


def _gw(
    client: MagicMock,
    *,
    params_required: bool = False,
    bound_params: BaseModel | None = None,
    param_namespace: str = "forze",
) -> PostgresReadGateway[_Row]:
    return PostgresReadGateway(
        relation=("public", "report"),
        client=client,
        model_type=_Row,
        codec=codec_for(_Row),
        introspector=_intro(),
        tenant_aware=False,
        params_required=params_required,
        bound_params=bound_params,
        param_namespace=param_namespace,
    )


def _rendered(stmt: Any) -> str:
    return stmt.as_string(None) if hasattr(stmt, "as_string") else str(stmt)


@pytest.mark.asyncio
async def test_bound_params_apply_set_config_in_tx() -> None:
    client = _client()
    gw = _gw(client, params_required=True, bound_params=_Params(window="2026-01-01"))

    await gw.find_many(None)

    assert client.transaction.called  # wrapped in a transaction
    assert client.execute.await_count == 1  # one set_config batch before the fetch
    rendered = _rendered(client.execute.await_args[0][0])
    assert "set_config" in rendered
    assert "forze.window" in rendered
    assert "2026-01-01" in rendered
    assert client.fetch_all.await_count == 1


@pytest.mark.asyncio
async def test_none_param_is_skipped_not_empty_string() -> None:
    client = _client()
    gw = _gw(
        client,
        params_required=True,
        bound_params=_OptionalParams(window="2026-01-01", region=None),
    )

    await gw.find_many(None)

    rendered = _rendered(client.execute.await_args[0][0])
    assert "forze.window" in rendered  # set value emitted
    assert "forze.region" not in rendered  # None skipped, not serialized to ''


@pytest.mark.asyncio
async def test_all_none_params_emit_no_set_config() -> None:
    client = _client()
    gw = _gw(
        client,
        params_required=True,
        bound_params=_OptionalParams(window=None, region=None),
    )

    await gw.find_many(None)

    assert client.execute.await_count == 0  # nothing to set
    assert client.fetch_all.await_count == 1  # but the read still runs (in its tx)


@pytest.mark.asyncio
async def test_unbound_required_fails_closed() -> None:
    client = _client()
    gw = _gw(client, params_required=True, bound_params=None)

    with pytest.raises(CoreException, match="query_parameters_unbound"):
        await gw.find_many(None)

    assert not client.fetch_all.called  # never reached the fetch


@pytest.mark.asyncio
async def test_no_params_reads_unaffected() -> None:
    client = _client()
    gw = _gw(client)  # not required, none bound

    await gw.find_many(None)

    assert client.execute.await_count == 0  # no set_config
    assert not client.transaction.called  # no extra tx wrapping
    assert client.fetch_all.await_count == 1


@pytest.mark.asyncio
async def test_custom_namespace_prefixes_setting() -> None:
    client = _client()
    gw = _gw(
        client,
        params_required=True,
        bound_params=_Params(window="w"),
        param_namespace="myapp",
    )

    await gw.find_many(None)

    assert "myapp.window" in _rendered(client.execute.await_args[0][0])


def test_adapter_with_parameters_returns_bound_clone() -> None:
    from forze.application.contracts.document import DocumentSpec
    from forze.application.integrations.document import DocumentCache
    from forze_postgres.adapters import PostgresDocumentAdapter

    gw = _gw(_client(), params_required=True)  # no params bound yet
    spec = DocumentSpec(name="report", read=_Row, query_params=_Params)
    cache = DocumentCache(
        read_model_type=_Row,
        read_codec=codec_for(_Row),
        document_name=spec.name,
        cache=None,
        after_commit=None,
    )
    adapter = PostgresDocumentAdapter(spec=spec, read_gw=gw, document_cache=cache)

    bound = adapter.with_parameters(_Params(window="2026-02-02"))

    assert bound is not adapter
    assert bound.read_gw.bound_params == _Params(window="2026-02-02")
    assert adapter.read_gw.bound_params is None  # original untouched
