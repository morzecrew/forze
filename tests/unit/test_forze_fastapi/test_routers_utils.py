"""Unit tests for forze_fastapi.endpoints._utils."""

from unittest.mock import MagicMock

import pytest

from forze.application.execution import Deps, ExecutionContext, make_registry_operation_resolver
from forze.application.execution.registry import FrozenOperationRegistry
from forze_fastapi.endpoints._utils import path_coerce

# ----------------------- #


class TestPathCoerce:
    """Tests for path_coerce."""

    def test_adds_leading_slash(self) -> None:
        assert path_coerce("items") == "/items"

    def test_preserves_leading_slash(self) -> None:
        assert path_coerce("/items") == "/items"

    def test_strips_trailing_slash(self) -> None:
        assert path_coerce("/items/") == "/items"
        assert path_coerce("items/") == "/items"


class TestMakeRegistryOperationResolver:
    def test_resolve_delegates_to_registry(self) -> None:
        registry = MagicMock(spec=FrozenOperationRegistry)
        handler = MagicMock()
        registry.resolve.return_value = handler
        ctx = ExecutionContext(deps=Deps())
        resolver = make_registry_operation_resolver(registry)

        out = resolver(ctx, "test.op")

        registry.resolve.assert_called_once_with("test.op", ctx)
        assert out is handler
