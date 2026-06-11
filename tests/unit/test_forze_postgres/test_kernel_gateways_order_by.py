"""Tests for :meth:`~forze_postgres.kernel.gateways.base.PostgresGateway.order_by_clause` direction validation."""

from unittest.mock import MagicMock

import pytest

from forze.base.exceptions import CoreException
from forze.domain.models import Document

from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.gateways import PostgresGateway
from tests.unit._gateway_codec_helpers import codec_for

# ----------------------- #


class _Doc(Document):
    sku: str


def _gateway() -> PostgresGateway[_Doc]:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.cache_partition_key = None
    intro.get_column_types.return_value = {}

    return PostgresGateway(
        relation=("public", "items"),
        client=MagicMock(),
        model_type=_Doc,
        codec=codec_for(_Doc),
        introspector=intro,
        tenant_aware=False,
    )


@pytest.mark.asyncio
async def test_order_by_renders_asc_and_desc() -> None:
    gw = _gateway()

    clause = await gw.order_by_clause({"sku": "asc", "id": "desc"})

    assert clause is not None
    rendered = clause.as_string()
    assert '"sku" ASC' in rendered
    assert '"id" DESC' in rendered


@pytest.mark.asyncio
async def test_order_by_empty_sorts_is_none() -> None:
    gw = _gateway()

    assert await gw.order_by_clause(None) is None
    assert await gw.order_by_clause({}) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "direction",
    [
        "ASC; DROP TABLE items",
        "asc, sku; --",
        "ASC",  # strict: only lowercase literals from the contract
        "descending",
        "",
    ],
)
async def test_order_by_rejects_non_whitelisted_direction(direction: str) -> None:
    gw = _gateway()

    with pytest.raises(CoreException, match="Invalid sort direction"):
        await gw.order_by_clause({"sku": direction})  # type: ignore[dict-item]
