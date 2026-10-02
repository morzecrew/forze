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


def test_hub_leg_order_limit_keeps_the_best_scores_and_breaks_ties() -> None:
    # A vector score is the negated distance, so the best is the highest there too.
    frag = hub_leg_order_limit(per_leg_limit=100).as_string()

    assert frag == ' ORDER BY "s" DESC NULLS LAST, "eid" LIMIT 100'
