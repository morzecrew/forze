"""Tests for tracing metadata inference."""

from __future__ import annotations

from forze.application.contracts.deps import DepKey
from forze.application.execution.tracing.port_proxy import infer_port_metadata

# ----------------------- #


class TestInferPortMetadata:
    def test_document_query(self) -> None:
        key = DepKey[object]("document_query")
        domain, surface, route, phase = infer_port_metadata(
            key,
            type("S", (), {"name": "projects"})(),
            route=None,
        )
        assert domain == "document"
        assert surface == "document_query"
        assert route == "projects"
        assert phase == "query"

    def test_document_command(self) -> None:
        key = DepKey[object]("document_command")
        domain, surface, route, phase = infer_port_metadata(
            key,
            type("S", (), {"name": "items"})(),
            route=None,
        )
        assert phase == "command"
        assert route == "items"

    def test_search_query(self) -> None:
        key = DepKey[object]("search_query")
        domain, surface, _, phase = infer_port_metadata(key, object(), route="r")
        assert domain == "search"
        assert surface == "search_query"
        assert phase == "query"

    def test_analytics_query(self) -> None:
        key = DepKey[object]("analytics_query")
        domain, _, _, phase = infer_port_metadata(key, object(), route=None)
        assert domain == "analytics"
        assert phase == "query"

    def test_storage_no_phase(self) -> None:
        key = DepKey[object]("storage")
        domain, surface, route, phase = infer_port_metadata(
            key,
            object(),
            route="bucket-a",
        )
        assert domain == "storage"
        assert surface == "storage"
        assert route == "bucket-a"
        assert phase is None

    def test_cache_no_phase(self) -> None:
        key = DepKey[object]("cache")
        domain, _, _, phase = infer_port_metadata(key, object(), route="c")
        assert domain == "cache"
        assert phase is None
