"""Tests for generated storage routes (forze_fastapi.routes.attach_storage_routes)."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from forze.application.contracts.storage import StorageSpec
from forze.base.exceptions import CoreException
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.routes import attach_storage_routes
from forze_kits.aggregates.storage import StorageKernelOp, build_storage_registry
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_modules

# ----------------------- #


def _build_app(style="rest", *, state: MockState | None = None, include=None) -> FastAPI:
    spec = StorageSpec(name="files")
    state = state or MockState()

    router = APIRouter(prefix="/files")
    attach_storage_routes(
        router,
        registry=build_storage_registry(spec).freeze(),
        ns=spec.default_namespace,
        ctx_dep=lambda: context_from_modules(MockDepsModule(state=state)),
        style=style,
        include=include,
    )

    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)

    return app


def _operation_ids(app: FastAPI) -> set[str]:
    return {
        operation["operationId"]
        for methods in app.openapi()["paths"].values()
        for operation in methods.values()
    }


# ....................... #


class TestStorageRoutes:
    def test_object_lifecycle_round_trip(self) -> None:
        client = TestClient(_build_app("rest"))

        uploaded = client.post(
            "/files",
            files={"file": ("hello.txt", b"hello world", "text/plain")},
            data={"description": "greeting", "prefix": "docs"},
        )
        assert uploaded.status_code == 201
        stored = uploaded.json()
        assert stored["filename"] == "hello.txt"
        assert stored["description"] == "greeting"

        listed = client.post("/files/list", json={})
        assert listed.status_code == 200
        assert [hit["key"] for hit in listed.json()["hits"]] == [stored["key"]]

        downloaded = client.get(f"/files/{stored['key']}")
        assert downloaded.status_code == 200
        assert downloaded.content == b"hello world"
        assert "hello.txt" in downloaded.headers["content-disposition"]

        deleted = client.delete(f"/files/{stored['key']}")
        assert deleted.status_code == 204

        assert client.post("/files/list", json={}).json()["hits"] == []

    def test_rpc_object_lifecycle_round_trip(self) -> None:
        client = TestClient(_build_app("rpc"))

        uploaded = client.post(
            "/files/upload",
            files={"file": ("hello.txt", b"hello world", "text/plain")},
        )
        assert uploaded.status_code == 200
        stored = uploaded.json()

        listed = client.post("/files/list", json={})
        assert [hit["key"] for hit in listed.json()["hits"]] == [stored["key"]]

        downloaded = client.get(f"/files/download/{stored['key']}")
        assert downloaded.status_code == 200
        assert downloaded.content == b"hello world"

        deleted = client.post(f"/files/delete/{stored['key']}")
        assert deleted.status_code == 204

        assert client.post("/files/list", json={}).json()["hits"] == []

    def test_keys_may_contain_slashes(self) -> None:
        client = TestClient(_build_app("rest"))

        stored = client.post(
            "/files",
            files={"file": ("a.txt", b"x", "text/plain")},
            data={"prefix": "nested/folder"},
        ).json()
        assert "/" in stored["key"]

        assert client.get(f"/files/{stored['key']}").status_code == 200
        assert client.delete(f"/files/{stored['key']}").status_code == 204

    def test_rpc_paths_are_operation_named(self) -> None:
        paths = _build_app("rpc").openapi()["paths"]

        assert set(paths) == {
            "/files/upload",
            "/files/list",
            "/files/download/{key}",
            "/files/delete/{key}",
        }

    @pytest.mark.parametrize("style", ["rest", "rpc"])
    def test_operation_ids_are_registry_keys_verbatim(self, style: str) -> None:
        expected = {f"files.{op.value}" for op in StorageKernelOp}

        assert _operation_ids(_build_app(style)) == expected

    def test_include_narrows_to_subset(self) -> None:
        app = _build_app("rest", include={StorageKernelOp.LIST})

        assert _operation_ids(app) == {"files.list"}

    def test_include_of_unknown_operation_raises(self) -> None:
        with pytest.raises(CoreException, match="Unknown operations"):
            _build_app("rest", include={"nope"})
