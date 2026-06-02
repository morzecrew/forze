"""Unit tests for :mod:`forze_postgres.adapters.search.hub._leg_sql`."""

from __future__ import annotations

import pytest

pytest.importorskip("psycopg")

from psycopg import sql

from forze_postgres.adapters.search.hub._leg_sql import build_hub_cte, hub_leg_order_limit
from forze_postgres.kernel.gateways import PostgresQualifiedName


def test_build_hub_cte_materialized() -> None:
    cols = sql.SQL("h.id")
    frag = build_hub_cte(
        hub_cols=cols,
        hub_rel_ident=PostgresQualifiedName("public", "hub").ident(),
        fw=sql.SQL("TRUE"),
        materialized=True,
    )
    assert "MATERIALIZED" in frag.as_string()


def test_hub_leg_order_limit_vector_asc() -> None:
    frag = hub_leg_order_limit(engine="vector", per_leg_limit=100)
    assert "ASC" in frag.as_string()
    assert "100" in frag.as_string()
