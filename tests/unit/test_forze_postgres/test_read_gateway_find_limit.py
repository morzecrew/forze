"""Implicit ``LIMIT`` + truncation signal on :class:`~forze_postgres.kernel.gateways.read.PostgresReadGateway`."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from forze.domain.models import Document
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client import PostgresClient
from forze_postgres.kernel.gateways import PostgresReadGateway
from tests.unit._gateway_codec_helpers import codec_for


class _Row(Document):
    title: str


def _client(rows: list | None = None) -> MagicMock:
    client = MagicMock(spec=PostgresClient)
    client.fetch_all = AsyncMock(return_value=rows or [])
    client.gather_concurrency_semaphore = MagicMock(return_value=asyncio.Semaphore(8))
    return client


def _gw(client: MagicMock, *, implicit_limit: int | None = 42) -> PostgresReadGateway[_Row]:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(return_value={})
    intro.cache_partition_key = None

    return PostgresReadGateway(
        relation=("public", "t"),
        client=client,
        model_type=_Row,
        codec=codec_for(_Row),
        introspector=intro,
        tenant_aware=False,
        find_many_implicit_limit=implicit_limit,
    )


@pytest.mark.asyncio
async def test_find_many_probes_one_past_the_implicit_cap() -> None:
    """When the caller omits ``limit``, the SQL fetches ``cap + 1`` so truncation is detectable."""

    client = _client()
    gw = _gw(client, implicit_limit=42)

    await gw.find_many(None, limit=None)

    _stmt, params, *_rest = client.fetch_all.await_args[0]
    assert params[-1] == 43  # cap + 1 probe row


@pytest.mark.asyncio
async def test_find_many_explicit_limit_is_not_probed() -> None:
    client = _client()
    gw = _gw(client, implicit_limit=42)

    await gw.find_many(None, limit=5)

    _stmt, params, *_rest = client.fetch_all.await_args[0]
    assert params[-1] == 5


def test_truncate_at_cap_warns_and_drops_the_probe_row(monkeypatch) -> None:
    from forze_postgres.kernel.gateways import read as read_mod

    fake_logger = MagicMock()
    monkeypatch.setattr(read_mod, "logger", fake_logger)

    gw = _gw(_client())
    rows = [{"i": i} for i in range(43)]  # cap (42) + 1 probe

    out = gw._truncate_at_cap(rows, 42)

    assert len(out) == 42  # truncated to the cap, probe dropped
    fake_logger.warning.assert_called_once()


def test_truncate_at_cap_silent_when_within_cap(monkeypatch) -> None:
    from forze_postgres.kernel.gateways import read as read_mod

    fake_logger = MagicMock()
    monkeypatch.setattr(read_mod, "logger", fake_logger)

    gw = _gw(_client())
    rows = [{"i": i} for i in range(42)]  # exactly the cap — not truncated

    assert gw._truncate_at_cap(rows, 42) == rows
    fake_logger.warning.assert_not_called()


def test_no_cap_leaves_rows_untouched() -> None:
    gw = _gw(_client(), implicit_limit=None)
    rows = [{"i": i} for i in range(100)]

    assert gw._truncate_at_cap(rows, None) is rows
