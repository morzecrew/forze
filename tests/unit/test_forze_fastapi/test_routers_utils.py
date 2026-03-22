"""Unit tests for forze_fastapi.endpoints._utils."""

from unittest.mock import MagicMock

import pytest
from fastapi import Depends
from starlette.testclient import TestClient

from forze.application.execution import Deps, ExecutionContext, UsecaseRegistry
from forze_fastapi.endpoints._utils import facade_dependency, path_coerce

# ----------------------- #


class TestPathCoerce:
    """Tests for path_coerce."""

    def test_adds_leading_slash(self) -> None:
        """Relative paths gain a leading slash."""
        assert path_coerce("items") == "/items"

    def test_preserves_leading_slash(self) -> None:
        """Already-absolute paths stay normalized without double slashes."""
        assert path_coerce("/items") == "/items"

    def test_strips_trailing_slash(self) -> None:
        """Trailing slash is removed for consistent route keys."""
        assert path_coerce("/items/") == "/items"
        assert path_coerce("items/") == "/items"


class TestFacadeDependency:
    """Tests for facade_dependency."""

    def test_resolves_facade_from_context(self) -> None:
        """Depends() wiring returns the facade built from registry + context."""

        class _Facade:
            def __init__(self, *, ctx: ExecutionContext, reg: UsecaseRegistry) -> None:
                self.ctx = ctx
                self.reg = reg

        reg = MagicMock(spec=UsecaseRegistry)
        ctx = ExecutionContext(deps=Deps())

        def ctx_dep():
            return ctx

        dep = facade_dependency(_Facade, reg, ctx_dep)

        from fastapi import FastAPI

        app = FastAPI()

        @app.get("/x")
        async def route(f: _Facade = Depends(dep)) -> dict:
            return {"same_ctx": f.ctx is ctx}

        client = TestClient(app)
        out = client.get("/x").json()
        assert out == {"same_ctx": True}

    def test_missing_context_dependency_fails_at_runtime(self) -> None:
        """If ctx_dep does not inject ExecutionContext, dependency resolution fails."""

        class _Facade:
            def __init__(self, *, ctx: ExecutionContext, reg: UsecaseRegistry) -> None:
                pass

        reg = MagicMock(spec=UsecaseRegistry)

        def bad_ctx_dep():
            raise RuntimeError("no ctx")

        dep = facade_dependency(_Facade, reg, bad_ctx_dep)

        from fastapi import FastAPI

        app = FastAPI()

        @app.get("/y")
        async def route(f: _Facade = Depends(dep)) -> dict:
            return {}

        client = TestClient(app, raise_server_exceptions=True)
        with pytest.raises(RuntimeError, match="no ctx"):
            client.get("/y")
