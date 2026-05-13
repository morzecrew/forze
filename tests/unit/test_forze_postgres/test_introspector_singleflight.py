"""Tests for introspector catalog single-flight coalescing."""

import asyncio
from unittest.mock import MagicMock

import pytest

pytest.importorskip("psycopg")

from forze_postgres.kernel.introspect import PostgresIntrospector


@pytest.mark.asyncio
async def test_get_relation_single_flights_concurrent_cold_loads() -> None:
    calls = {"n": 0}

    async def fetch_value(*_a: object, **_k: object) -> str:
        calls["n"] += 1
        await asyncio.sleep(0.04)
        return "r"

    client = MagicMock()
    client.fetch_value = fetch_value

    intro = PostgresIntrospector(client=client)

    await asyncio.gather(
        intro.get_relation(schema="public", relation="widgets"),
        intro.get_relation(schema="public", relation="widgets"),
    )

    assert calls["n"] == 1
