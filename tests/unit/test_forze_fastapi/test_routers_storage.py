"""Unit tests for forze_fastapi.endpoints.storage."""

from fastapi import APIRouter, FastAPI
from starlette.testclient import TestClient

from forze.application.composition.storage import build_storage_registry
from forze.application.contracts.storage import StorageSpec
from forze.application.execution import UsecasePlan, UsecaseRegistry
from forze_fastapi.endpoints.storage import attach_storage_endpoints
from forze_fastapi.exceptions import register_exception_handlers

# ----------------------- #

_FILES = StorageSpec(name="files")


def _build_registry() -> UsecaseRegistry:
    reg = build_storage_registry(_FILES).extend_plan(UsecasePlan().tx("*", route="mock"))
    reg.finalize(_FILES.name, inplace=True)
    return reg


class TestAttachStorageEndpoints:
    def test_attaches_expected_paths(self, composition_ctx) -> None:
        reg = _build_registry()

        def ctx_dep():
            return composition_ctx

        router = APIRouter(prefix="/store")
        attach_storage_endpoints(
            router,
            registry=reg,
            ctx_dep=ctx_dep,
            storage=_FILES,
            endpoints={
                "list_": True,
                "upload": True,
                "download": True,
                "delete": True,
            },
        )

        paths_methods = {
            (r.path, tuple(sorted(r.methods)))
            for r in router.routes
            if hasattr(r, "path") and hasattr(r, "methods")
        }
        assert ("/store/list", ("POST",)) in paths_methods
        assert ("/store/upload", ("POST",)) in paths_methods
        assert ("/store/download/{key:path}", ("GET",)) in paths_methods
        assert ("/store/delete/{key:path}", ("DELETE",)) in paths_methods

    def test_list_upload_download_delete_roundtrip(self, composition_ctx) -> None:
        reg = _build_registry()

        def ctx_dep():
            return composition_ctx

        router = APIRouter(prefix="/store")
        attach_storage_endpoints(
            router,
            registry=reg,
            ctx_dep=ctx_dep,
            storage=_FILES,
            endpoints={
                "list_": True,
                "upload": True,
                "download": True,
                "delete": True,
            },
        )

        app = FastAPI()
        register_exception_handlers(app)
        app.include_router(router)
        client = TestClient(app)

        up = client.post(
            "/store/upload",
            data={"description": "d1", "prefix": "p"},
            files={"file": ("hello.txt", b"hello", "text/plain")},
        )
        assert up.status_code == 200
        body = up.json()
        key = body["key"]
        assert body["filename"] == "hello.txt"
        assert body["size"] == 5

        listed = client.post("/store/list", json={"page": 1, "size": 10})
        assert listed.status_code == 200
        lj = listed.json()
        assert lj["count"] >= 1
        assert len(lj["hits"]) >= 1

        dl = client.get(f"/store/download/{key}")
        assert dl.status_code == 200
        assert dl.content == b"hello"
        assert "text/plain" in (dl.headers.get("content-type") or "")

        rm = client.delete(f"/store/delete/{key}")
        assert rm.status_code == 204

        missing = client.get(f"/store/download/{key}")
        assert missing.status_code == 404

    def test_skips_disabled_endpoints(self, composition_ctx) -> None:
        reg = _build_registry()

        def ctx_dep():
            return composition_ctx

        router = APIRouter(prefix="/store")
        attach_storage_endpoints(
            router,
            registry=reg,
            ctx_dep=ctx_dep,
            storage=_FILES,
            endpoints={"list_": True, "upload": False},
        )

        paths = {r.path for r in router.routes if hasattr(r, "path")}
        assert "/store/list" in paths
        assert "/store/upload" not in paths
