"""Unit tests for ``forze_postgres.execution.deps`` (module, factories, utils, config validation)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from forze.base.exceptions import CoreException
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from pydantic import BaseModel

pytest.importorskip("psycopg")

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.contracts.embeddings import EmbeddingsProviderDepKey
from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.contracts.secrets import SecretRef
from forze.application.contracts.transaction.deps import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.adapters import (
    PostgresDocumentAdapter,
    PostgresFTSSearchAdapter,
    PostgresPGroongaSearchAdapter,
    PostgresVectorSearchAdapter,
)
from forze_postgres.adapters.txmanager import PostgresTxManagerAdapter
from forze_postgres.execution.deps.configs import (
    FtsEngine,
    PgroongaEngine,
    PostgresDocumentConfig,
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
    PostgresReadOnlyDocumentConfig,
    PostgresSearchConfig,
    VectorEngine,
)
from forze_postgres.execution.deps import (
    ConfigurablePostgresDocument,
    ConfigurablePostgresReadOnlyDocument,
    ConfigurablePostgresSearch,
    postgres_txmanager,
)
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.execution.deps.module import PostgresDepsModule
from forze_postgres.execution.deps.utils import doc_write_gw, read_gw
from forze_postgres.kernel.gateways import PostgresReadGateway, PostgresWriteGateway
from forze_postgres.kernel.catalog.hub_fk_columns import normalize_hub_fk_columns
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client import RoutedPostgresClient
from forze_postgres.kernel.client.client import PostgresClient


class _R(ReadDocument):
    title: str


class _D(Document):
    title: str


class _C(CreateDocumentCmd):
    title: str


class _U(BaseDTO):
    title: str | None = None


def _rw_spec(*, history_enabled: bool = False) -> DocumentSpec:
    return DocumentSpec(
        name="dep_test",
        read=_R,
        write={
            "domain": _D,
            "create_cmd": _C,
            "update_cmd": _U,
        },
        history_enabled=history_enabled,
    )


def _ctx() -> ExecutionContext:
    client = MagicMock(spec=PostgresClient)
    intro = MagicMock(spec=PostgresIntrospector)
    return context_from_deps(Deps.plain(
            {
                PostgresClientDepKey: client,
                PostgresIntrospectorDepKey: intro,
                EmbeddingsProviderDepKey: lambda _c, _s: MagicMock(),
            }
        )
    )


class TestPostgresSearchConfigValidation:
    def test_pgroonga_skips_fts_validation(self) -> None:
        PostgresSearchConfig(
            engine="pgroonga",
            index=("public", "idx"),
            read=("public", "src"),
        )

    def test_fts_requires_groups(self) -> None:
        with pytest.raises(CoreException, match="FTS groups are required"):
            FtsEngine(groups={})

    def test_fts_rejects_duplicate_fields_across_groups(self) -> None:
        with pytest.raises(CoreException, match="duplicate"):
            PostgresSearchConfig(
                engine=FtsEngine(groups={"A": ["a", "b"], "B": ["b"]}),
                index=("public", "idx"),
                read=("public", "src"),
            )


class TestPostgresDepsModule:
    def test_registers_client_and_introspector(self) -> None:
        client = MagicMock(spec=PostgresClient)
        module = PostgresDepsModule(client=client)

        deps = module()

        assert isinstance(deps, Deps)
        assert deps.exists(PostgresClientDepKey)
        assert deps.exists(PostgresIntrospectorDepKey)

    def test_introspector_receives_cache_ttl(self) -> None:
        from datetime import timedelta

        client = MagicMock(spec=PostgresClient)
        ttl = timedelta(minutes=2)
        module = PostgresDepsModule(client=client, introspector_cache_ttl=ttl)
        ctx = context_from_deps(module())
        intro = ctx.deps.provide(PostgresIntrospectorDepKey)
        assert intro.cache_ttl == ttl

    def test_registers_read_only_document_routes(self) -> None:
        client = MagicMock(spec=PostgresClient)
        module = PostgresDepsModule(
            client=client,
            ro_documents={
                "ro": PostgresReadOnlyDocumentConfig(read=("public", "only_read")),
            },
        )

        deps = module()
        assert deps.exists(DocumentQueryDepKey, route="ro")

    def test_registers_rw_documents_search_and_tx(self) -> None:
        client = MagicMock(spec=PostgresClient)
        module = PostgresDepsModule(
            client=client,
            rw_documents={
                "rw_route": PostgresDocumentConfig(
                    read=("public", "docs"),
                    write=("public", "docs"),
                    bookkeeping_strategy="application",
                ),
            },
            searches={
                "find": PostgresSearchConfig(
                    engine="pgroonga",
                    index=("public", "idx_find"),
                    read=("public", "src_find"),
                ),
            },
            tx={"main"},
        )

        deps = module()

        assert deps.exists(DocumentQueryDepKey, route="rw_route")
        assert deps.exists(DocumentCommandDepKey, route="rw_route")
        assert deps.exists(SearchQueryDepKey, route="find")
        assert deps.exists(TransactionManagerDepKey, route="main")

    def test_invalid_fts_search_config_fails_at_build_time(self) -> None:
        with pytest.raises(CoreException, match="FTS groups are required"):
            PostgresSearchConfig(
                engine=FtsEngine(groups={}),
                index=("public", "i"),
                read=("public", "s"),
            )

    def test_routed_client_requires_introspector_partition_key(self) -> None:
        from uuid import UUID

        tid = UUID("11111111-1111-1111-1111-111111111111")
        secrets = MagicMock()
        routed = RoutedPostgresClient(
            secrets=secrets,
            secret_ref_for_tenant=lambda t: SecretRef(path=f"tenants/{t}/dsn"),
            tenant_provider=lambda: tid,
        )

        with pytest.raises(CoreException, match="postgres_tenancy_validation_failed"):
            PostgresDepsModule(client=routed)

    def test_routed_client_with_partition_key_builds(self) -> None:
        from uuid import UUID

        tid = UUID("11111111-1111-1111-1111-111111111111")
        secrets = MagicMock()
        routed = RoutedPostgresClient(
            secrets=secrets,
            secret_ref_for_tenant=lambda t: SecretRef(path=f"tenants/{t}/dsn"),
            tenant_provider=lambda: tid,
        )

        module = PostgresDepsModule(
            client=routed,
            introspector_cache_partition_key=lambda: str(tid),
        )
        deps = module()
        assert deps.exists(PostgresClientDepKey)


class TestConfigurablePostgresDocumentFactories:
    def test_read_only_builds_query_adapter_without_write_gateway(self) -> None:
        factory = ConfigurablePostgresReadOnlyDocument(
            config=PostgresReadOnlyDocumentConfig(read=("public", "v_docs")),
        )
        ctx = _ctx()
        spec = DocumentSpec(name="x", read=_R)

        adapter = factory(ctx, spec)

        assert isinstance(adapter, PostgresDocumentAdapter)
        assert adapter.write_gw is None
        assert adapter.read_gw.relation == ("public", "v_docs")

    def test_read_only_builds_adapter_with_batch_size(self) -> None:
        factory = ConfigurablePostgresReadOnlyDocument(
            config=PostgresReadOnlyDocumentConfig(
                read=("public", "v_docs"),
                batch_size=321,
            ),
        )
        ctx = _ctx()
        adapter = factory(ctx, DocumentSpec(name="x", read=_R))

        assert isinstance(adapter, PostgresDocumentAdapter)
        assert adapter.batch_size == 321

    def test_rejects_mapping_config(self) -> None:
        with pytest.raises(TypeError, match="PostgresDocumentConfig"):
            ConfigurablePostgresDocument(
                config={
                    "read": ("public", "t"),
                    "write": ("public", "t"),
                    "bookkeeping_strategy": "application",
                },
            )

    def test_command_requires_write_spec(self) -> None:
        factory = ConfigurablePostgresDocument(
            config=PostgresDocumentConfig(
                read=("public", "t"),
                write=("public", "t"),
                bookkeeping_strategy="application",
            )
        )
        ctx = _ctx()
        spec = DocumentSpec(name="no_write", read=_R)

        with pytest.raises(CoreException, match="Write relation is required"):
            factory(ctx, spec)

    def test_command_builds_adapter_with_batch_size(self) -> None:
        factory = ConfigurablePostgresDocument(
            config=PostgresDocumentConfig(
                read=("public", "t"),
                write=("public", "t"),
                bookkeeping_strategy="application",
                batch_size=333,
            )
        )
        ctx = _ctx()
        adapter = factory(ctx, _rw_spec())

        assert isinstance(adapter, PostgresDocumentAdapter)
        assert adapter.batch_size == 333
        assert adapter.write_gw is not None

    def test_builds_when_history_enabled_but_no_history_relation(self) -> None:
        factory = ConfigurablePostgresDocument(
            config=PostgresDocumentConfig(
                read=("public", "t"),
                write=("public", "t"),
                bookkeeping_strategy="application",
            )
        )
        ctx = _ctx()
        adapter = factory(ctx, _rw_spec(history_enabled=True))

        assert adapter.write_gw is not None
        assert adapter.write_gw.history_gw is None


class TestConfigurablePostgresSearch:
    def _search_spec(self) -> SearchSpec:
        class M(BaseModel):
            title: str

        return SearchSpec(name="s", model_type=M, fields=["title"])

    def test_pgroonga_branch(self) -> None:
        factory = ConfigurablePostgresSearch(
            config=PostgresSearchConfig(
                engine="pgroonga",
                index=("public", "gi"),
                read=("public", "gs"),
            )
        )
        ctx = _ctx()
        out = factory(ctx, self._search_spec())

        assert isinstance(out, PostgresPGroongaSearchAdapter)

    def test_fts_branch(self) -> None:
        factory = ConfigurablePostgresSearch(
            config=PostgresSearchConfig(
                engine=FtsEngine(groups={"A": ["title"]}),
                index=("public", "fi"),
                read=("public", "fs"),
            )
        )
        ctx = _ctx()
        out = factory(ctx, self._search_spec())

        assert isinstance(out, PostgresFTSSearchAdapter)

    def test_vector_branch(self) -> None:
        factory = ConfigurablePostgresSearch(
            config=PostgresSearchConfig(
                engine=VectorEngine(
                    column="vector_column",
                    dimensions=1234,
                    embeddings_name="embeddings_name",
                ),
                index=("public", "vi"),
                read=("public", "vs"),
            )
        )
        ctx = _ctx()
        out = factory(ctx, self._search_spec())

        assert isinstance(out, PostgresVectorSearchAdapter)

    def test_fts_missing_groups_in_call_raises(self) -> None:
        with pytest.raises(CoreException, match="FTS groups are required"):
            ConfigurablePostgresSearch(
                config=PostgresSearchConfig(
                    engine=FtsEngine(groups={}),
                    index=("public", "fi"),
                    read=("public", "fs"),
                )
            )

    def test_fts_validate_groups_requires_all_search_fields(self) -> None:
        class M(BaseModel):
            title: str
            body: str

        spec = SearchSpec(name="s", model_type=M, fields=["title", "body"])
        factory = ConfigurablePostgresSearch(
            config=PostgresSearchConfig(
                engine=FtsEngine(groups={"A": ["title"]}),
                index=("public", "fi"),
                read=("public", "fs"),
            )
        )
        ctx = _ctx()

        with pytest.raises(CoreException, match="All search fields must be included"):
            factory(ctx, spec)

    def test_pgroonga_invalid_score_version_validates(self) -> None:
        with pytest.raises(CoreException, match="pgroonga_score_version"):
            PgroongaEngine(score_version="bad")  # type: ignore[arg-type]

    def test_pgroonga_score_version_v1_builds(self) -> None:
        factory = ConfigurablePostgresSearch(
            config=PostgresSearchConfig(
                engine=PgroongaEngine(score_version="v1"),
                index=("public", "gi"),
                read=("public", "gs"),
            )
        )
        ctx = _ctx()
        out = factory(ctx, self._search_spec())
        assert out.pgroonga_score_version == "v1"

    def test_read_validation_trusted_wires_to_adapter(self) -> None:
        factory = ConfigurablePostgresSearch(
            config=PostgresSearchConfig(
                engine="pgroonga",
                index=("public", "gi"),
                read=("public", "gs"),
                read_validation="trusted",
            )
        )
        ctx = _ctx()
        out = factory(ctx, self._search_spec())

        assert out.read_validation == "trusted"


def test_postgres_txmanager_builds_adapter() -> None:
    ctx = _ctx()
    tx = postgres_txmanager(ctx)

    assert isinstance(tx, PostgresTxManagerAdapter)


def test_read_gw_factory() -> None:
    ctx = _ctx()
    gw = read_gw(
        ctx,
        read_type=_R,
        read_relation=("public", "rel_a"),
        tenant_aware=True,
    )

    assert isinstance(gw, PostgresReadGateway)
    assert gw.relation == ("public", "rel_a")
    assert gw.tenant_aware is True


def test_doc_write_gw_without_history() -> None:
    ctx = _ctx()
    spec = _rw_spec()
    gw = doc_write_gw(
        ctx,
        write_types=spec.write,  # type: ignore[arg-type]
        codecs=spec.resolved_codecs,
        write_relation=("public", "w"),
        history_relation=None,
        history_enabled=False,
        bookkeeping_strategy="application",
        tenant_aware=False,
    )

    assert isinstance(gw, PostgresWriteGateway)
    assert gw.history_gw is None


def test_doc_write_gw_with_history() -> None:
    ctx = _ctx()
    spec = _rw_spec(history_enabled=True)
    gw = doc_write_gw(
        ctx,
        write_types=spec.write,  # type: ignore[arg-type]
        codecs=spec.resolved_codecs,
        write_relation=("public", "w"),
        history_relation=("public", "h"),
        history_enabled=True,
        bookkeeping_strategy="application",
        tenant_aware=False,
    )

    assert gw.history_gw is not None
    assert gw.history_gw.relation == ("public", "h")


def test_normalize_hub_fk_columns_str_and_sequence() -> None:
    assert normalize_hub_fk_columns("a") == ("a",)
    assert normalize_hub_fk_columns(["x", "y"]) == ("x", "y")


def test_normalize_hub_fk_columns_rejects_empty_and_dupes() -> None:
    with pytest.raises(CoreException, match="at least one column"):
        normalize_hub_fk_columns([])

    with pytest.raises(CoreException, match="unique within a leg"):
        normalize_hub_fk_columns(("p", "p"))


def test_postgres_hub_search_config_accepts_single_leg() -> None:
    PostgresHubSearchConfig(
        hub=("public", "h"),
        members={
            "m1": PostgresHubSearchMemberConfig(
                index=("public", "i1"),
                read=("public", "t1"),
                engine="pgroonga",
                hub_fk="party_id",
            ),
        },
    )


def test_postgres_hub_search_config_rejects_empty_members() -> None:
    with pytest.raises(CoreException, match="at least one leg"):
        PostgresHubSearchConfig(
            hub=("public", "h"),
            members={},
        )


def test_postgres_hub_search_config_accepts_hub_fk_list() -> None:
    PostgresHubSearchConfig(
        hub=("public", "h"),
        members={
            "m1": PostgresHubSearchMemberConfig(
                index=("public", "i1"),
                read=("public", "t1"),
                engine="pgroonga",
                hub_fk=["a", "b"],
            ),
            "m2": PostgresHubSearchMemberConfig(
                index=("public", "i2"),
                read=("public", "t2"),
                engine="pgroonga",
                hub_fk="c",
            ),
        },
    )


def test_postgres_hub_search_config_duplicate_hub_fk() -> None:
    with pytest.raises(CoreException, match="duplicate column across legs"):
        PostgresHubSearchConfig(
            hub=("public", "h"),
            members={
                "m1": PostgresHubSearchMemberConfig(
                    index=("public", "i1"),
                    read=("public", "t1"),
                    engine="pgroonga",
                    hub_fk="x",
                ),
                "m2": PostgresHubSearchMemberConfig(
                    index=("public", "i2"),
                    read=("public", "t2"),
                    engine="pgroonga",
                    hub_fk="x",
                ),
            },
        )


def test_postgres_hub_search_config_duplicate_hub_fk_within_leg() -> None:
    with pytest.raises(CoreException, match="unique within a leg"):
        PostgresHubSearchConfig(
            hub=("public", "h"),
            members={
                "m1": PostgresHubSearchMemberConfig(
                    index=("public", "i1"),
                    read=("public", "t1"),
                    engine="pgroonga",
                    hub_fk=("x", "x"),
                ),
                "m2": PostgresHubSearchMemberConfig(
                    index=("public", "i2"),
                    read=("public", "t2"),
                    engine="pgroonga",
                    hub_fk="y",
                ),
            },
        )


def test_postgres_hub_search_config_list_overlaps_other_leg() -> None:
    with pytest.raises(CoreException, match="duplicate column across legs"):
        PostgresHubSearchConfig(
            hub=("public", "h"),
            members={
                "m1": PostgresHubSearchMemberConfig(
                    index=("public", "i1"),
                    read=("public", "t1"),
                    engine="pgroonga",
                    hub_fk=["a", "b"],
                ),
                "m2": PostgresHubSearchMemberConfig(
                    index=("public", "i2"),
                    read=("public", "t2"),
                    engine="pgroonga",
                    hub_fk="b",
                ),
            },
        )


def test_postgres_hub_search_config_fts_requires_fts_groups() -> None:
    with pytest.raises(CoreException, match="FTS groups are required"):
        PostgresHubSearchMemberConfig(
            index=("public", "i1"),
            read=("public", "t1"),
            hub_fk="a",
            engine=FtsEngine(groups={}),
        )


def test_postgres_hub_search_config_same_heap_as_hub_ok() -> None:
    PostgresHubSearchConfig(
        hub=("public", "h"),
        members={
            "m1": PostgresHubSearchMemberConfig(
                index=("public", "i1"),
                read=("public", "h"),
                engine="pgroonga",
                hub_fk="id",
                same_heap_as_hub=True,
            ),
        },
    )


def test_postgres_hub_search_config_same_heap_as_hub_mismatched_read() -> None:
    with pytest.raises(CoreException, match="same qualified relation"):
        PostgresHubSearchConfig(
            hub=("public", "h"),
            members={
                "m1": PostgresHubSearchMemberConfig(
                    index=("public", "i1"),
                    read=("public", "other"),
                    engine="pgroonga",
                    hub_fk="id",
                    same_heap_as_hub=True,
                ),
            },
        )


def test_postgres_hub_search_config_same_heap_as_hub_fk_not_pk() -> None:
    with pytest.raises(CoreException, match="heap_pk"):
        PostgresHubSearchConfig(
            hub=("public", "h"),
            members={
                "m1": PostgresHubSearchMemberConfig(
                    index=("public", "i1"),
                    read=("public", "h"),
                    engine="pgroonga",
                    hub_fk="ref_id",
                    heap_pk="id",
                    same_heap_as_hub=True,
                ),
            },
        )


def test_postgres_hub_search_config_same_heap_as_hub_fts() -> None:
    with pytest.raises(CoreException, match="same_heap_as_hub with engine 'fts'"):
        PostgresHubSearchConfig(
            hub=("public", "h"),
            members={
                "m1": PostgresHubSearchMemberConfig(
                    index=("public", "i1"),
                    read=("public", "h"),
                    hub_fk="id",
                    engine=FtsEngine(groups={"A": ("a",)}),
                    same_heap_as_hub=True,
                ),
            },
        )


def test_postgres_hub_search_config_same_heap_as_hub_field_map() -> None:
    with pytest.raises(CoreException, match="field_map"):
        PostgresHubSearchConfig(
            hub=("public", "h"),
            members={
                "m1": PostgresHubSearchMemberConfig(
                    index=("public", "i1"),
                    read=("public", "h"),
                    engine="pgroonga",
                    hub_fk="id",
                    field_map={"a": "b"},
                    same_heap_as_hub=True,
                ),
            },
        )


def test_postgres_hub_search_config_same_heap_as_hub_pgroonga_v1() -> None:
    with pytest.raises(CoreException, match="v2"):
        PostgresHubSearchConfig(
            hub=("public", "h"),
            members={
                "m1": PostgresHubSearchMemberConfig(
                    index=("public", "i1"),
                    read=("public", "h"),
                    engine=PgroongaEngine(score_version="v1"),
                    hub_fk="id",
                    same_heap_as_hub=True,
                ),
            },
        )
