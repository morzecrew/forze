"""Micro-benchmark for the Postgres gateway ``return_clause`` column-list cache.

Perf tier (``@pytest.mark.perf``): excluded from ``just test``; run via ``just perf``.
In-process only (no Docker) — uses a mock client/introspector.

``return_clause`` is built on every SELECT (get/find/find_many) and every create
RETURNING. The non-projection cases (default read fields / explicit ``return_type``)
are now memoized per ``(return_type, alias)``; ``_build_return_clause`` is the
pre-cache path that rebuilds the column-list composable each call.

Run::

    just perf tests/perf/test_forze_postgres_gateway_perf.py
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from forze.domain.models import Document
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.gateways import PostgresGateway
from tests.unit._gateway_codec_helpers import codec_for


class _Doc(Document):
    sku: str
    name: str
    description: str
    price: int
    category: str


def _gateway() -> PostgresGateway[_Doc]:
    intro = MagicMock(spec=PostgresIntrospector)
    intro.cache_partition_key = None

    return PostgresGateway(
        relation=("public", "items"),
        client=MagicMock(),
        model_type=_Doc,
        codec=codec_for(_Doc),
        introspector=intro,
        tenant_aware=False,
    )


@pytest.mark.perf
def test_return_clause_cached_benchmark(benchmark: Any) -> None:
    """Cached column list: a per-gateway dict hit after the first build."""

    gw = _gateway()

    benchmark(lambda: gw.return_clause())


@pytest.mark.perf
def test_return_clause_uncached_benchmark(benchmark: Any) -> None:
    """Pre-cache path: rebuild the column-list composable on every call."""

    gw = _gateway()
    use = list(gw.read_fields)

    benchmark(lambda: gw._build_return_clause(use, None))


# In-process and deterministic: participates in the CI perf regression gate.
pytestmark = pytest.mark.perf_gate
