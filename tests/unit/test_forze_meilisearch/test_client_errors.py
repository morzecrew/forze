"""Unit tests for :mod:`forze_meilisearch.kernel.client.errors`."""

import pytest

pytest.importorskip("meilisearch")

from meilisearch_python_sdk.errors import (
    MeilisearchApiError,
    MeilisearchCommunicationError,
    MeilisearchTimeoutError,
)

from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_meilisearch.kernel.client.errors import _meilisearch_eh


class TestMeilisearchErrorHandler:
    def test_core_error_passthrough(self) -> None:
        original = exc.internal("x")
        assert _meilisearch_eh(original, site="search") is original

    @pytest.mark.parametrize(
        ("error", "needle"),
        [
            (MeilisearchTimeoutError("timeout"), "timeout"),
            (MeilisearchCommunicationError("network"), "communication"),
            (MeilisearchApiError("api", 400, "bad"), "API error"),
        ],
    )
    def test_infrastructure_errors(self, error: BaseException, needle: str) -> None:
        mapped = _meilisearch_eh(error, site="index")
        assert isinstance(mapped, CoreException)
        assert mapped.kind == ExceptionKind.INFRASTRUCTURE
        assert needle in mapped.summary

    def test_unknown_exception_fallback(self) -> None:
        mapped = _meilisearch_eh(RuntimeError("boom"), site="op")
        assert isinstance(mapped, CoreException)
        assert mapped.kind == ExceptionKind.INFRASTRUCTURE
        assert "op" in mapped.summary
        # raw driver text must not leak into the summary, only into details
        assert "boom" not in mapped.summary
        assert mapped.details is not None
        assert mapped.details["error"] == "boom"


class TestAssembledChain:
    """Regression: the package mapper must be reachable through the chain
    wired into ``exc_interceptor`` (nested default chain used to shadow it)."""

    def test_timeout_through_assembled_chain(self) -> None:
        from forze_meilisearch.kernel.client.errors import exc_interceptor

        out = exc_interceptor.mapper(MeilisearchTimeoutError("timeout"), site="index")
        assert out is not None
        assert out.code != "core.unhandled"
        assert "Meilisearch" in out.summary
