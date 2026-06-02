"""Unit tests for :mod:`forze_postgres.adapters.search._search_count`."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

pytest.importorskip("psycopg")

from psycopg import sql

from forze_postgres.adapters.search._search_count import resolve_ranked_approximate_total


@pytest.mark.asyncio
async def test_resolve_ranked_approximate_total_clamps_combo_limit() -> None:
    intro = AsyncMock()
    intro.estimate_filtered_rows = AsyncMock(return_value=10_000)

    total = await resolve_ranked_approximate_total(
        introspector=intro,
        schema="public",
        relation="hub",
        where_sql=sql.SQL("TRUE"),
        params=[],
        combo_limit=100,
    )

    assert total == 100
    intro.estimate_filtered_rows.assert_awaited_once()
