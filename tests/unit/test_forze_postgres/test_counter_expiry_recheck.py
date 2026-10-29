"""The Postgres counter's memory of a table found without ``expires_at`` — no DB.

The catalog read is replaced, so these pin only whose answer the re-check marker is.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest

from forze.application.contracts.counter import counter_capabilities
from forze.base.exceptions import CoreException
from forze_postgres.adapters.counter import PostgresCounterAdapter
from forze_postgres.execution.deps.configs import PostgresCounterConfig
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client import PostgresClient

pytestmark = pytest.mark.unit

# ----------------------- #


async def test_an_absent_column_marker_belongs_to_its_introspector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two runtimes on different databases share a partition and a table name, not an answer."""

    rereads: list[PostgresIntrospector] = []

    async def no_columns(self: PostgresIntrospector, **_where: str) -> dict[str, str]:
        return {}

    monkeypatch.setattr(PostgresIntrospector, "get_column_types", no_columns)
    monkeypatch.setattr(
        PostgresIntrospector, "invalidate_relation", lambda self, **_where: rereads.append(self)
    )
    config = PostgresCounterConfig(relation=("public", f"counters_{uuid4().hex[:8]}"))
    one, two = (
        PostgresCounterAdapter(
            client=(client := PostgresClient()),
            config=config,
            route="orders",
            introspector=PostgresIntrospector(client=client),
        )
        for _ in range(2)
    )

    with pytest.raises(CoreException):
        await one.incr(ttl=timedelta(minutes=1))

    assert counter_capabilities(one).expiry is False
    assert counter_capabilities(two).expiry is True, "the other database's answer"

    with pytest.raises(CoreException):
        await two.incr(ttl=timedelta(minutes=1))

    assert rereads == [one.introspector, two.introspector], "each re-read its own catalog"


def test_a_per_tenant_table_reports_expiry_without_a_catalog_read() -> None:
    counter = PostgresCounterAdapter(
        client=PostgresClient(),
        config=PostgresCounterConfig(relation=lambda tenant: ("public", f"counters_{tenant}")),
        route="orders",
    )

    assert counter_capabilities(counter).expiry is True
