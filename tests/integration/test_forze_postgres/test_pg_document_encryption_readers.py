"""A document's sealed column, read back by the planes that query its table in place.

The document port binds each ciphertext to the document spec's name. An analytics query or
an in-place search over the same table opens it only when its spec's ``sealed_by`` names that
document; without the name the rows are refused, never accepted under another namespace. Each
reader runs in its own context (its own keyring), so every read is cold.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsQueryDefinition, AnalyticsSpec
from forze.application.contracts.crypto import FieldEncryption, KeyRef, StaticKeyDirectory
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.search import HubSearchSpec, SearchSpec
from forze.application.execution import CryptoDepsModule, Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockKeyManagement
from forze_postgres.execution import PostgresDepsModule
from forze_postgres.execution.deps.configs import (
    FtsEngine,
    PostgresAnalyticsConfig,
    PostgresDocumentConfig,
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
    PostgresQueryConfig,
    PostgresSearchConfig,
)
from forze_postgres.kernel.client import PostgresClient
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.integration

# ----------------------- #


class _Vault(Document):
    title: str
    secret: str


class _VaultCreate(CreateDocumentCmd):
    title: str
    secret: str


class _VaultUpdate(BaseDTO):
    secret: str | None = None


class _VaultRead(ReadDocument):
    title: str
    secret: str


class _Report(BaseModel):
    id: str
    title: str
    secret: str


class _Params(BaseModel):
    pass


_POLICY = FieldEncryption(encrypted=frozenset({"secret"}))
_BOUND = FieldEncryption(encrypted=frozenset({"secret"}), binds_record_id=True)


def _specs(
    sealed_by: str | None = None,
    policy: FieldEncryption = _POLICY,
) -> tuple[
    DocumentSpec[_VaultRead, _Vault, _VaultCreate, _VaultUpdate],
    AnalyticsSpec[_Report, _Report],
    SearchSpec[_VaultRead],
]:
    return (
        DocumentSpec(
            name="vault",
            read=_VaultRead,
            write={  # type: ignore[arg-type]
                "domain": _Vault,
                "create_cmd": _VaultCreate,
                "update_cmd": _VaultUpdate,
            },
            encryption=policy,
        ),
        AnalyticsSpec(
            name="vault_report",
            read=_Report,
            queries={
                "all": AnalyticsQueryDefinition(params=_Params),
                "no_id": AnalyticsQueryDefinition(params=_Params),
            },
            encryption=policy,
            sealed_by=sealed_by,
        ),
        SearchSpec(
            name="vault",
            model_type=_VaultRead,
            fields=["title"],
            encryption=policy,
            sealed_by=sealed_by,
        ),
    )


def _hub(
    sealed_by: str | None = None, policy: FieldEncryption = _POLICY
) -> HubSearchSpec[_VaultRead]:
    """A hub over the document table: its hub rows are decoded under the hub's sealed_by."""

    leg = SearchSpec(name="vault_leg", model_type=_VaultRead, fields=["title"])
    return HubSearchSpec(
        name="vault_hub",
        model_type=_VaultRead,
        members=[leg],
        encryption=policy,
        sealed_by=sealed_by,
    )


def _ctx(pg_client: PostgresClient, table: str) -> ExecutionContext:
    """A fresh context with its OWN keyring (a separate process, so reads start cold)."""

    module = PostgresDepsModule(
        client=pg_client,
        rw_documents={
            "vault": PostgresDocumentConfig(
                read=("public", table),
                write=("public", table),
                bookkeeping_strategy="application",
            )
        },
        analytics={
            "vault_report": PostgresAnalyticsConfig(
                queries={
                    "all": PostgresQueryConfig(
                        # table name is a test-local literal, never caller input
                        sql=f"SELECT id::text AS id, title, secret FROM public.{table}",
                    ),
                    "no_id": PostgresQueryConfig(
                        sql=f"SELECT title, secret FROM public.{table}",
                    ),
                }
            )
        },
        searches={
            "vault": PostgresSearchConfig(
                engine=FtsEngine(groups={"A": ("title",)}),
                index=("public", f"idx_{table}"),
                read=("public", table),
            )
        },
        hub_searches={
            "vault_hub": PostgresHubSearchConfig(
                hub=("public", table),
                members={
                    "vault_leg": PostgresHubSearchMemberConfig(
                        engine=FtsEngine(groups={"A": ("title",)}),
                        index=("public", f"idx_{table}"),
                        read=("public", table),
                        hub_fk="id",
                    )
                },
            )
        },
    )
    crypto = CryptoDepsModule(
        kms=MockKeyManagement(), directory=StaticKeyDirectory(KeyRef(key_id="vault-cmk"))
    )
    return context_from_deps(Deps.merge(crypto(), module()))


async def _write_sealed(pg_client: PostgresClient, policy: FieldEncryption = _POLICY) -> str:
    table = f"vault_{uuid4().hex[:12]}"
    # table name is a test-local literal, never caller input
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            secret text NOT NULL
        );
        CREATE INDEX idx_{table} ON public.{table}
            USING gin (to_tsvector('english', coalesce(title, '')));
        """
    )
    document, _, _ = _specs(sealed_by="vault", policy=policy)
    writer = _ctx(pg_client, table).document.command(document)
    await writer.create(_VaultCreate(title="ledger one", secret="plain-1"))

    stored = await pg_client.fetch_one(f"SELECT secret FROM public.{table}")
    assert stored is not None and "plain-1" not in stored["secret"], "secret stored unsealed"
    return table


# ....................... #


@pytest.mark.asyncio
async def test_readers_open_the_document_rows_when_sealed_by_names_it(
    pg_client: PostgresClient,
) -> None:
    table = await _write_sealed(pg_client)
    _, analytics, search = _specs(sealed_by="vault")

    page = await _ctx(pg_client, table).analytics.query(analytics).run_page("all", _Params())
    assert [hit.secret for hit in page.hits] == ["plain-1"]

    searcher = _ctx(pg_client, table).search.query(search)
    found = await searcher.search_page("ledger", pagination={"limit": 5})
    assert [hit.secret for hit in found.hits] == ["plain-1"]


@pytest.mark.asyncio
async def test_readers_refuse_the_document_rows_without_sealed_by(
    pg_client: PostgresClient,
) -> None:
    table = await _write_sealed(pg_client)
    _, analytics, search = _specs()

    with pytest.raises(CoreException) as refused:
        await _ctx(pg_client, table).analytics.query(analytics).run_page("all", _Params())

    assert refused.value.code == "core.crypto.aead_auth_failed"

    searcher = _ctx(pg_client, table).search.query(search)

    with pytest.raises(CoreException) as refused:
        await searcher.search_page("ledger", pagination={"limit": 5})

    assert refused.value.code == "core.crypto.aead_auth_failed"


@pytest.mark.asyncio
@pytest.mark.parametrize("sealed_by", ["vault", None])
async def test_a_hub_decodes_the_document_rows_under_its_own_sealed_by(
    pg_client: PostgresClient, sealed_by: str | None
) -> None:
    table = await _write_sealed(pg_client)
    hub = _ctx(pg_client, table).search.hub(_hub(sealed_by=sealed_by))

    if sealed_by is not None:
        found = await hub.search_page("ledger", pagination={"limit": 5})
        assert [hit.secret for hit in found.hits] == ["plain-1"]
        return

    with pytest.raises(CoreException) as refused:
        await hub.search_page("ledger", pagination={"limit": 5})

    assert refused.value.code == "core.crypto.aead_auth_failed"


@pytest.mark.asyncio
async def test_id_bound_rows_open_only_when_the_read_returns_the_id(
    pg_client: PostgresClient,
) -> None:
    """A document that binds its record id: each reader opens the row with the id it reads
    back, and a query that does not return the id is refused rather than opened id-less."""

    table = await _write_sealed(pg_client, _BOUND)
    _, analytics, search = _specs(sealed_by="vault", policy=_BOUND)

    page = await _ctx(pg_client, table).analytics.query(analytics).run_page("all", _Params())
    assert [hit.secret for hit in page.hits] == ["plain-1"]

    found = await _ctx(pg_client, table).search.query(search).search_page(
        "ledger", pagination={"limit": 5}
    )
    assert [hit.secret for hit in found.hits] == ["plain-1"]

    hub = _ctx(pg_client, table).search.hub(_hub(sealed_by="vault", policy=_BOUND))
    found = await hub.search_page("ledger", pagination={"limit": 5})
    assert [hit.secret for hit in found.hits] == ["plain-1"]

    query = _ctx(pg_client, table).analytics.query(analytics)

    with pytest.raises(CoreException) as refused:
        await query.run_page("no_id", _Params())

    assert refused.value.code == "core.crypto.record_id_required"
