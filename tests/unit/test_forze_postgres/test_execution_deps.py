"""Unit tests for ``forze_postgres.execution.deps`` (module, factories, utils, config validation)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

pytest.importorskip("psycopg")

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.contracts.tx import TxManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import CoreError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.adapters import (
    PostgresDocumentAdapter,
    PostgresFTSSearchAdapter,
    PostgresPGroongaSearchAdapterV2,
)
from forze_postgres.adapters.txmanager import PostgresTxManagerAdapter
from forze_postgres.execution.deps.configs import (
    PostgresHubSearchConfig,
    validate_pg_search_conf,
    validate_postgres_hub_search_conf,
)
from forze_postgres.execution.deps.deps import (
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
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient


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
    return ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: client,
                PostgresIntrospectorDepKey: intro,
            }
        )
    )


class TestValidatePgSearchConf:
    def test_pgroonga_skips_fts_validation(self) -> None:
        validate_pg_search_conf(
            {
                "engine": "pgroonga",
                "index": ("public", "idx"),
                "source": ("public", "src"),
            }
        )

    def test_fts_requires_groups(self) -> None:
        with pytest.raises(CoreError, match="FTS groups are required"):
            validate_pg_search_conf(
                {
                    "engine": "fts",
                    "index": ("public", "idx"),
                    "source": ("public", "src"),
                }
            )

    def test_fts_rejects_duplicate_fields_across_groups(self) -> None:
        with pytest.raises(CoreError, match="duplicate"):
            validate_pg_search_conf(
                {
                    "engine": "fts",
                    "index": ("public", "idx"),
                    "source": ("public", "src"),
                    "fts_groups": {"A": ["a", "b"], "B": ["b"]},
                }
            )


class TestPostgresDepsModule:
    def test_registers_client_and_introspector(self) -> None:
        client = MagicMock(spec=PostgresClient)
        module = PostgresDepsModule(client=client)

        deps = module()

        assert isinstance(deps, Deps)
        assert deps.exists(PostgresClientDepKey)
        assert deps.exists(PostgresIntrospectorDepKey)

    def test_registers_read_only_document_routes(self) -> None:
        client = MagicMock(spec=PostgresClient)
        module = PostgresDepsModule(
            client=client,
            ro_documents={
                "ro": {"read": ("public", "only_read")},
            },
        )

        deps = module()
        assert deps.exists(DocumentQueryDepKey, route="ro")

    def test_registers_rw_documents_search_and_tx(self) -> None:
        client = MagicMock(spec=PostgresClient)
        module = PostgresDepsModule(
            client=client,
            rw_documents={
                "rw_route": {
                    "read": ("public", "docs"),
                    "write": ("public", "docs"),
                    "bookkeeping_strategy": "application",
                },
            },
            searches={
                "find": {
                    "engine": "pgroonga",
                    "index": ("public", "idx_find"),
                    "source": ("public", "src_find"),
                },
            },
            tx={"main"},
        )

        deps = module()

        assert deps.exists(DocumentQueryDepKey, route="rw_route")
        assert deps.exists(DocumentCommandDepKey, route="rw_route")
        assert deps.exists(SearchQueryDepKey, route="find")
        assert deps.exists(TxManagerDepKey, route="main")

    def test_invalid_fts_search_config_fails_at_build_time(self) -> None:
        client = MagicMock(spec=PostgresClient)
        module = PostgresDepsModule(
            client=client,
            searches={
                "bad": {
                    "engine": "fts",
                    "index": ("public", "i"),
                    "source": ("public", "s"),
                },
            },
        )

        with pytest.raises(CoreError, match="FTS groups are required"):
            module()


class TestConfigurablePostgresDocumentFactories:
    def test_read_only_builds_query_adapter_without_write_gateway(self) -> None:
        factory = ConfigurablePostgresReadOnlyDocument(
            config={"read": ("public", "v_docs")},
        )
        ctx = _ctx()
        spec = DocumentSpec(name="x", read=_R)

        adapter = factory(ctx, spec)

        assert isinstance(adapter, PostgresDocumentAdapter)
        assert adapter.write_gw is None
        assert adapter.read_gw.source_qname.schema == "public"
        assert adapter.read_gw.source_qname.name == "v_docs"

    def test_command_requires_write_spec(self) -> None:
        factory = ConfigurablePostgresDocument(
            config={
                "read": ("public", "t"),
                "write": ("public", "t"),
                "bookkeeping_strategy": "application",
            }
        )
        ctx = _ctx()
        spec = DocumentSpec(name="no_write", read=_R)

        with pytest.raises(CoreError, match="Write relation is required"):
            factory(ctx, spec)

    def test_command_builds_adapter_with_batch_size(self) -> None:
        factory = ConfigurablePostgresDocument(
            config={
                "read": ("public", "t"),
                "write": ("public", "t"),
                "bookkeeping_strategy": "application",
                "batch_size": 333,
            }
        )
        ctx = _ctx()
        adapter = factory(ctx, _rw_spec())

        assert isinstance(adapter, PostgresDocumentAdapter)
        assert adapter.batch_size == 333
        assert adapter.write_gw is not None

    def test_builds_when_history_enabled_but_no_history_relation(self) -> None:
        factory = ConfigurablePostgresDocument(
            config={
                "read": ("public", "t"),
                "write": ("public", "t"),
                "bookkeeping_strategy": "application",
            }
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
            config={
                "engine": "pgroonga",
                "index": ("public", "gi"),
                "read": ("public", "gs"),
            }
        )
        ctx = _ctx()
        out = factory(ctx, self._search_spec())

        assert isinstance(out, PostgresPGroongaSearchAdapterV2)

    def test_fts_branch(self) -> None:
        factory = ConfigurablePostgresSearch(
            config={
                "engine": "fts",
                "index": ("public", "fi"),
                "read": ("public", "fs"),
                "fts_groups": {"A": ["title"]},
            }
        )
        ctx = _ctx()
        out = factory(ctx, self._search_spec())

        assert isinstance(out, PostgresFTSSearchAdapter)

    def test_fts_missing_groups_in_call_raises(self) -> None:
        factory = ConfigurablePostgresSearch(
            config={
                "engine": "fts",
                "index": ("public", "fi"),
                "read": ("public", "fs"),
            }
        )
        ctx = _ctx()

        with pytest.raises(CoreError, match="FTS groups are required"):
            factory(ctx, self._search_spec())

    def test_fts_validate_groups_requires_all_search_fields(self) -> None:
        class M(BaseModel):
            title: str
            body: str

        spec = SearchSpec(name="s", model_type=M, fields=["title", "body"])
        factory = ConfigurablePostgresSearch(
            config={
                "engine": "fts",
                "index": ("public", "fi"),
                "read": ("public", "fs"),
                "fts_groups": {"A": ["title"]},
            }
        )
        ctx = _ctx()

        with pytest.raises(CoreError, match="All search fields must be included"):
            factory(ctx, spec)


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
    assert gw.source_qname.string() == "public.rel_a"
    assert gw.tenant_aware is True


def test_doc_write_gw_without_history() -> None:
    ctx = _ctx()
    gw = doc_write_gw(
        ctx,
        write_types=_rw_spec().write,  # type: ignore[arg-type]
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
    gw = doc_write_gw(
        ctx,
        write_types=_rw_spec().write,  # type: ignore[arg-type]
        write_relation=("public", "w"),
        history_relation=("public", "h"),
        history_enabled=True,
        bookkeeping_strategy="application",
        tenant_aware=False,
    )

    assert gw.history_gw is not None
    assert gw.history_gw.source_qname.name == "h"


def test_validate_postgres_hub_search_conf_duplicate_hub_fk() -> None:
    cfg: PostgresHubSearchConfig = {
        "hub": ("public", "h"),
        "members": {
            "m1": {
                "index": ("public", "i1"),
                "read": ("public", "t1"),
                "hub_fk": "x",
            },
            "m2": {
                "index": ("public", "i2"),
                "read": ("public", "t2"),
                "hub_fk": "x",
            },
        },
    }
    with pytest.raises(CoreError, match="hub_fk_column must be unique"):
        validate_postgres_hub_search_conf(cfg)
