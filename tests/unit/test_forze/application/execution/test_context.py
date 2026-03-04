"""Unit tests for ExecutionContext."""

import pytest

from forze.application.contracts.deps import DepKey
from forze.application.execution import Deps
from forze.application.execution import ExecutionContext

# ----------------------- #


class TestExecutionContextDep:
    """Tests for ExecutionContext.dep() dependency resolution."""

    def test_dep_resolves_registered(self) -> None:
        deps = Deps(deps={DepKey[str]("foo"): "bar"})
        ctx = ExecutionContext(deps=deps)
        assert ctx.dep(DepKey[str]("foo")) == "bar"

    def test_dep_resolves_typed(self) -> None:
        deps = Deps(deps={DepKey[int]("num"): 42})
        ctx = ExecutionContext(deps=deps)
        result: int = ctx.dep(DepKey[int]("num"))
        assert result == 42

    def test_dep_missing_raises(self) -> None:
        from forze.base.errors import CoreError

        ctx = ExecutionContext(deps=Deps())
        with pytest.raises(CoreError, match="not found"):
            ctx.dep(DepKey[str]("missing"))

    def test_dep_cycle_detected_raises(self) -> None:
        from forze.base.errors import CoreError

        from forze.application.contracts.deps import DepsPort

        ctx_ref: list[ExecutionContext] = []

        class CyclicDeps(DepsPort):
            def provide(self, key):
                if key.name == "cycle_a":
                    return ctx_ref[0].dep(DepKey("cycle_b"))
                return ctx_ref[0].dep(DepKey("cycle_a"))

        deps = CyclicDeps()
        ctx = ExecutionContext(deps=deps)
        ctx_ref.append(ctx)

        with pytest.raises(CoreError, match="cycle"):
            ctx.dep(DepKey("cycle_a"))


class TestExecutionContextConvenienceMethods:
    """Tests for doc, cache, counter, txmanager, storage, search."""

    def test_cache_resolves(self) -> None:
        from forze.application.contracts.cache import CacheDepKey, CacheSpec

        def cache_factory(ctx, spec):
            return object()

        deps = Deps(
            deps={
                CacheDepKey: cache_factory,
            }
        )
        ctx = ExecutionContext(deps=deps)
        result = ctx.cache(CacheSpec(namespace="test"))
        assert result is not None

    def test_counter_resolves(self) -> None:
        from forze.application.contracts.counter import CounterDepKey

        def counter_factory(ctx, namespace):
            return object()

        deps = Deps(deps={CounterDepKey: counter_factory})
        ctx = ExecutionContext(deps=deps)
        result = ctx.counter("ns")
        assert result is not None

    def test_txmanager_resolves(self) -> None:
        from forze.application.contracts.tx import TxManagerDepKey

        def tx_factory(ctx):
            return object()

        deps = Deps(deps={TxManagerDepKey: tx_factory})
        ctx = ExecutionContext(deps=deps)
        result = ctx.txmanager()
        assert result is not None

    def test_storage_resolves(self) -> None:
        from forze.application.contracts.storage import StorageDepKey

        def storage_factory(ctx, bucket):
            return object()

        deps = Deps(deps={StorageDepKey: storage_factory})
        ctx = ExecutionContext(deps=deps)
        result = ctx.storage("bucket")
        assert result is not None

    def test_search_resolves(self) -> None:
        from forze.application.contracts.search import SearchReadDepKey
        from forze.application.contracts.search.internal import SearchIndexSpec, SearchSpec
        from forze.application.contracts.search.internal.specs import SearchFieldSpec

        def search_factory(ctx, spec):
            return object()

        deps = Deps(deps={SearchReadDepKey: search_factory})
        ctx = ExecutionContext(deps=deps)
        spec = SearchSpec(
            indexes={"main": SearchIndexSpec(fields=[SearchFieldSpec(path="id")])},
            default_index="main",
        )
        result = ctx.search(spec)
        assert result is not None

    def test_doc_resolves_without_cache(self) -> None:
        from datetime import timedelta

        from forze.application.contracts.document import DocumentDepKey, DocumentSpec
        from forze.domain.models import CreateDocumentCmd, Document, ReadDocument

        spec = DocumentSpec(
            namespace="test",
            sources={"read": "r", "write": "w"},
            models={
                "read": ReadDocument,
                "domain": Document,
                "create_cmd": CreateDocumentCmd,
                "update_cmd": CreateDocumentCmd,
            },
            cache=None,
        )

        def doc_factory(ctx, s, cache=None):
            return object()

        deps = Deps(deps={DocumentDepKey: doc_factory})
        ctx = ExecutionContext(deps=deps)
        result = ctx.doc(spec)
        assert result is not None

    def test_doc_resolves_with_cache_enabled(self) -> None:
        from datetime import timedelta

        from forze.application.contracts.cache import CacheDepKey, CacheSpec
        from forze.application.contracts.document import DocumentDepKey, DocumentSpec
        from forze.domain.models import CreateDocumentCmd, Document, ReadDocument

        spec = DocumentSpec(
            namespace="test",
            sources={"read": "r", "write": "w"},
            models={
                "read": ReadDocument,
                "domain": Document,
                "create_cmd": CreateDocumentCmd,
                "update_cmd": CreateDocumentCmd,
            },
            cache={"enabled": True, "ttl": timedelta(seconds=60)},
        )

        def doc_factory(ctx, s, cache=None):
            return object()

        def cache_factory(ctx, s):
            return object()

        deps = Deps(
            deps={
                DocumentDepKey: doc_factory,
                CacheDepKey: cache_factory,
            }
        )
        ctx = ExecutionContext(deps=deps)
        result = ctx.doc(spec)
        assert result is not None
