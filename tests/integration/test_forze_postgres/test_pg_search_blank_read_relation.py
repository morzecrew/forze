"""A blank query reads the adapter's read relation, as every ranked query does.

The factory reads and ranks from one relation, so only an adapter given a ``read_relation``
of its own tells the two apart: a blank page, its total, a blank cursor and a blank aggregate
must all read that projection rather than the relation the gateway was built over.
"""

from __future__ import annotations

from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps
from forze.domain.models import ReadDocument
from forze_postgres.execution.deps import ConfigurablePostgresSearch
from forze_postgres.execution.deps.configs import FtsEngine, PostgresSearchConfig
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

# ----------------------- #


class _Read(ReadDocument):
    title: str


@pytest.mark.asyncio
async def test_a_blank_query_reads_the_read_relation(pg_client: PostgresClient) -> None:
    t = f"brel_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL
        );
        CREATE INDEX idx_{t} ON {t} USING gin (to_tsvector('english', coalesce(title, '')));
        INSERT INTO {t} VALUES
            (gen_random_uuid(), 1, now(), now(), 'alpha one'),
            (gen_random_uuid(), 1, now(), now(), 'alpha two'),
            (gen_random_uuid(), 1, now(), now(), 'hidden alpha');
        CREATE VIEW {t}_shown AS SELECT * FROM {t} WHERE title NOT LIKE 'hidden%';
        """
    )
    deps = Deps.plain(
        {
            PostgresClientDepKey: pg_client,
            PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
            SearchQueryDepKey: ConfigurablePostgresSearch(
                config=PostgresSearchConfig(
                    engine=FtsEngine(groups={"A": ("title",)}),
                    index=("public", f"idx_{t}"),
                    read=("public", t),
                )
            ),
        }
    )
    spec = SearchSpec(name=t, model_type=_Read, fields=["title"])
    port = attrs.evolve(
        context_from_deps(deps).search.query(spec),  # type: ignore[misc]
        read_relation=("public", f"{t}_shown"),
    )
    shown = ["alpha one", "alpha two"]

    ranked = await port.search_page("alpha", pagination={"limit": 10})
    assert sorted(hit.title for hit in ranked.hits) == shown

    blank = await port.search_page("", pagination={"limit": 10})
    assert sorted(hit.title for hit in blank.hits) == shown
    assert blank.count == 2

    cursor = await port.search_cursor("", cursor={"limit": 10})
    assert sorted(hit.title for hit in cursor.hits) == shown

    counted = await port.aggregate_search({"$computed": {"n": {"$count": None}}}, "")
    assert [row["n"] for row in counted.hits] == [2]
