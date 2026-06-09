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

    def test_unknown_exception_returns_none(self) -> None:
        assert _meilisearch_eh(RuntimeError("boom"), site="op") is None
