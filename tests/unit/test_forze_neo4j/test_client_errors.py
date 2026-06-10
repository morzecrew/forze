"""Unit tests for :mod:`forze_neo4j.kernel.client.errors`."""

import pytest

pytest.importorskip("neo4j")

from neo4j.exceptions import ConstraintError, ServiceUnavailable, TransientError

from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_neo4j.kernel.client.errors import _neo4j_eh, exc_interceptor

# ----------------------- #


class TestNeo4jErrorHandler:
    def test_core_error_passthrough(self) -> None:
        original = exc.internal("x")
        assert _neo4j_eh(original, site="op") is original

    def test_transient_error_maps_to_concurrency(self) -> None:
        out = _neo4j_eh(TransientError("deadlock"), site="run")
        assert isinstance(out, CoreException)
        assert out.kind == ExceptionKind.CONCURRENCY

    def test_constraint_error_maps_to_conflict(self) -> None:
        out = _neo4j_eh(ConstraintError("unique"), site="merge")
        assert isinstance(out, CoreException)
        assert out.kind == ExceptionKind.CONFLICT

    def test_service_unavailable_maps_to_infrastructure(self) -> None:
        out = _neo4j_eh(ServiceUnavailable("down"), site="connect")
        assert isinstance(out, CoreException)
        assert out.kind == ExceptionKind.INFRASTRUCTURE


class TestAssembledChain:
    """Drive the actual chain wired into ``exc_interceptor``.

    Regression: the nested default chain used to make ``_neo4j_eh``
    unreachable, so transient errors surfaced as INTERNAL "Unhandled
    exception" instead of CONCURRENCY and were never retried.
    """

    def test_transient_error_maps_to_concurrency(self) -> None:
        out = exc_interceptor.mapper(TransientError("deadlock"), site="run")
        assert out is not None
        assert out.kind == ExceptionKind.CONCURRENCY

    def test_constraint_error_maps_to_conflict(self) -> None:
        out = exc_interceptor.mapper(ConstraintError("unique"), site="merge")
        assert out is not None
        assert out.kind == ExceptionKind.CONFLICT

    def test_unknown_exception_reaches_package_fallback(self) -> None:
        out = exc_interceptor.mapper(RuntimeError("weird"), site="op")
        assert out is not None
        assert out.kind == ExceptionKind.INFRASTRUCTURE
        assert out.code != "core.unhandled"
