"""A blank-query page decrypts sealed fields and refuses a sort on them, as a ranked page does.

A blank query reads the read projection directly rather than through the ranked pipeline, so
it must apply the same two rules the ranked offset page applies to every row it returns.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import FieldEncryption, KeyRef, StaticKeyDirectory
from forze.application.contracts.search import (
    SearchQueryDepKey,
    SearchResultSnapshotDepKey,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from forze.application.execution import CryptoDepsModule, Deps
from forze.base.exceptions import CoreException
from forze.domain.models import ReadDocument
from forze_mock import MockKeyManagement
from forze_postgres.execution.deps import ConfigurablePostgresSearch
from forze_postgres.execution.deps.configs import FtsEngine, PostgresSearchConfig
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from forze_redis.execution.deps import ConfigurableRedisSearchResultSnapshot
from forze_redis.execution.deps.configs import RedisSearchResultSnapshotConfig
from forze_redis.execution.deps.keys import RedisClientDepKey
from forze_redis.kernel.client import RedisClient
from tests.support.execution_context import context_from_deps

# ----------------------- #


class _Read(ReadDocument):
    title: str
    secret: str


class _Proj(BaseModel):
    id: UUID
    secret: str


_ENC = FieldEncryption(encrypted=frozenset({"secret"}))
_INDEXES = {
    "fts": "USING gin (to_tsvector('english', coalesce(title, '')))",
    "pgroonga": "USING pgroonga (title)",
}


@pytest.mark.asyncio
@pytest.mark.parametrize("engine", ["fts", "pgroonga"])
async def test_a_blank_page_decrypts_and_guards_its_sort(
    pg_client: PostgresClient, engine: str
) -> None:
    await _check(pg_client, engine, redis_client=None)


@pytest.mark.asyncio
@pytest.mark.parametrize("engine", ["fts", "pgroonga"])
async def test_a_blank_page_written_to_a_snapshot_decrypts(
    pg_client: PostgresClient, redis_client: RedisClient, engine: str
) -> None:
    await _check(pg_client, engine, redis_client=redis_client)


async def _check(
    pg_client: PostgresClient, engine: str, *, redis_client: RedisClient | None
) -> None:
    snapshotted = redis_client is not None

    if engine == "pgroonga":
        await pg_client.execute("CREATE EXTENSION IF NOT EXISTS pgroonga;")

    t = f"senc_{engine}_{uuid4().hex[:8]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            secret text NOT NULL
        );
        CREATE INDEX idx_{t} ON {t} {_INDEXES[engine]};
        """
    )
    kms = MockKeyManagement()
    deps = Deps.merge(
        CryptoDepsModule(kms=kms, directory=StaticKeyDirectory(KeyRef(key_id="k1")))(),
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                **(
                    {
                        RedisClientDepKey: redis_client,
                        SearchResultSnapshotDepKey: ConfigurableRedisSearchResultSnapshot(
                            config=RedisSearchResultSnapshotConfig(namespace=f"it:senc:{t}"),
                        ),
                    }
                    if snapshotted
                    else {}
                ),
                SearchQueryDepKey: ConfigurablePostgresSearch(
                    config=PostgresSearchConfig(
                        engine=(
                            FtsEngine(groups={"A": ("title",)}) if engine == "fts" else "pgroonga"
                        ),
                        index=("public", f"idx_{t}"),
                        read=("public", t),
                    )
                ),
            }
        ),
    )
    spec = (
        SearchSpec(
            name=t,
            model_type=_Read,
            fields=["title"],
            encryption=_ENC,
            snapshot=(
                SearchResultSnapshotSpec(
                    name="snap", enabled=True, ttl=timedelta(minutes=5), max_ids=100
                )
                if snapshotted
                else None
            ),
        )
    )
    # Sealed through a codec of its own, so the port's reads start with no key warmed.
    codec = context_from_deps(deps).search.query(spec).spec.resolved_read_codec
    port = context_from_deps(deps).search.query(spec)
    await codec.prepare_encrypt()
    rid = uuid4()
    sealed = codec.encrypt_mapping({"id": str(rid), "secret": "plain-1"}, record_id=str(rid))
    await pg_client.execute(
        f"INSERT INTO {t} (id, rev, created_at, last_update_at, title, secret) "
        "VALUES (%(id)s, 1, now(), now(), 'ledger one', %(s)s)",
        {"id": rid, "s": sealed["secret"]},
    )

    # Blank first: its read is the one that meets the key cold.
    for query in ("", "ledger"):
        written = await port.search_page(query, pagination={"limit": 5})
        assert written.hits[0].secret == "plain-1", (engine, query)

        if snapshotted:
            # Written through the snapshot stream, then served back from it.
            assert written.snapshot is not None, (engine, query)
            replayed = await port.search_page(
                query,
                pagination={"limit": 5},
                snapshot={"id": written.snapshot.id, "fingerprint": written.snapshot.fingerprint},
            )
            assert replayed.hits[0].secret == "plain-1", (engine, query)

        if not snapshotted:
            # A projection on a snapshot-enabled spec is refused for a reason of its own.
            projected = await port.project_search(["id", "secret"], query)
            assert projected.hits[0]["secret"] == "plain-1", (engine, query)
            selected = await port.select_search(_Proj, query)
            assert selected.hits[0].secret == "plain-1", (engine, query)

        with pytest.raises(CoreException) as refused:
            await port.search(query, None, None, {"secret": "asc"})

        assert refused.value.code == "core.search.encrypted_sort_field", (engine, query)
