"""Tests for generated storage routes (forze_fastapi.routes.attach_storage_routes)."""

from __future__ import annotations

import inspect
from typing import Any

import pytest

pytest.importorskip("fastapi")

import starlette.datastructures
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from forze.application.contracts.storage import StorageSpec
from forze.base.exceptions import CoreException
from forze_fastapi.exceptions import ERROR_CODE_HEADER, register_exception_handlers
from forze_fastapi.routes import DEFAULT_MAX_UPLOAD_SIZE, attach_storage_routes
from forze_kits.aggregates.storage import StorageKernelOp, build_storage_registry
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_modules

# ----------------------- #

_UNSET: Any = object()


def _build_app(
    style="rest",
    *,
    state: MockState | None = None,
    include=None,
    max_upload_size: int | None = _UNSET,
) -> FastAPI:
    spec = StorageSpec(name="files")
    state = state or MockState()

    kwargs = {} if max_upload_size is _UNSET else {"max_upload_size": max_upload_size}

    router = APIRouter(prefix="/files")
    attach_storage_routes(
        router,
        registry=build_storage_registry(spec).freeze(),
        ns=spec.default_namespace,
        ctx_dep=lambda: context_from_modules(MockDepsModule(state=state)),
        style=style,
        include=include,
        **kwargs,
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

        deleted = client.delete(f"/files/delete/{stored['key']}")
        assert deleted.status_code == 204

        assert client.post("/files/list", json={}).json()["hits"] == []

    def test_download_filename_is_header_safe(self) -> None:
        client = TestClient(_build_app("rest"))

        stored = client.post(
            "/files",
            files={"file": ('evil"\r\nX-Injected: 1.txt', b"x", "text/plain")},
        ).json()

        downloaded = client.get(f"/files/{stored['key']}")

        assert downloaded.status_code == 200
        assert "X-Injected" not in downloaded.headers
        disposition = downloaded.headers["content-disposition"]
        assert "\r" not in disposition and "\n" not in disposition
        assert disposition.startswith("attachment; filename*=utf-8''")

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
        assert set(paths["/files/upload"]) == {"post"}
        assert set(paths["/files/download/{key}"]) == {"get"}
        assert set(paths["/files/delete/{key}"]) == {"delete"}

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


# ....................... #


class TestStorageDownloadRangeAndConditional:
    """Range / conditional support on the generated download route (edge-only)."""

    def _upload(self, client: TestClient) -> dict:
        return client.post(
            "/files",
            files={"file": ("big.bin", b"0123456789", "application/octet-stream")},
        ).json()

    def test_plain_download_is_unchanged_200(self) -> None:
        client = TestClient(_build_app("rest"))
        stored = self._upload(client)

        resp = client.get(f"/files/{stored['key']}")

        assert resp.status_code == 200
        assert resp.content == b"0123456789"
        assert "content-range" not in resp.headers
        # ETag/Accept-Ranges are additive, not a behavior change.
        assert resp.headers["accept-ranges"] == "bytes"
        assert resp.headers["etag"]

    def test_range_request_returns_206_partial(self) -> None:
        client = TestClient(_build_app("rest"))
        stored = self._upload(client)

        resp = client.get(
            f"/files/{stored['key']}", headers={"Range": "bytes=2-5"}
        )

        assert resp.status_code == 206
        assert resp.content == b"2345"
        assert resp.headers["content-range"] == "bytes 2-5/10"

    def test_open_ended_range(self) -> None:
        client = TestClient(_build_app("rest"))
        stored = self._upload(client)

        resp = client.get(
            f"/files/{stored['key']}", headers={"Range": "bytes=7-"}
        )

        assert resp.status_code == 206
        assert resp.content == b"789"
        assert resp.headers["content-range"] == "bytes 7-9/10"

    def test_suffix_range(self) -> None:
        client = TestClient(_build_app("rest"))
        stored = self._upload(client)

        resp = client.get(
            f"/files/{stored['key']}", headers={"Range": "bytes=-3"}
        )

        assert resp.status_code == 206
        assert resp.content == b"789"
        assert resp.headers["content-range"] == "bytes 7-9/10"

    def test_unsatisfiable_range_returns_416(self) -> None:
        client = TestClient(_build_app("rest"))
        stored = self._upload(client)

        resp = client.get(
            f"/files/{stored['key']}", headers={"Range": "bytes=99-"}
        )

        assert resp.status_code == 416
        assert resp.headers["content-range"] == "bytes */10"

    def test_if_none_match_matching_etag_returns_304(self) -> None:
        client = TestClient(_build_app("rest"))
        stored = self._upload(client)

        full = client.get(f"/files/{stored['key']}")
        etag = full.headers["etag"]

        resp = client.get(
            f"/files/{stored['key']}", headers={"If-None-Match": etag}
        )

        assert resp.status_code == 304
        assert resp.content == b""
        assert resp.headers["etag"] == etag

    def test_if_none_match_stale_etag_returns_200(self) -> None:
        client = TestClient(_build_app("rest"))
        stored = self._upload(client)

        resp = client.get(
            f"/files/{stored['key']}", headers={"If-None-Match": '"stale"'}
        )

        assert resp.status_code == 200
        assert resp.content == b"0123456789"


class TestStorageUploadCap:
    def test_default_cap_is_64_mib(self) -> None:
        assert DEFAULT_MAX_UPLOAD_SIZE == 64 * 1024 * 1024

        parameters = inspect.signature(attach_storage_routes).parameters
        assert parameters["max_upload_size"].default is DEFAULT_MAX_UPLOAD_SIZE

    def test_upload_below_cap_succeeds(self) -> None:
        client = TestClient(_build_app("rest", max_upload_size=1024))

        uploaded = client.post(
            "/files",
            files={"file": ("small.txt", b"hello", "text/plain")},
        )

        assert uploaded.status_code == 201
        assert uploaded.json()["filename"] == "small.txt"

    def test_upload_above_cap_is_rejected_with_standard_payload(self) -> None:
        state = MockState()
        client = TestClient(_build_app("rest", state=state, max_upload_size=10))

        uploaded = client.post(
            "/files",
            files={"file": ("big.bin", b"x" * 1000, "application/octet-stream")},
        )

        assert uploaded.status_code == 422
        assert "maximum allowed size" in uploaded.json()["detail"]
        assert uploaded.headers[ERROR_CODE_HEADER] == "upload_too_large"

        # The operation pipeline (and storage port) was never invoked.
        assert client.post("/files/list", json={}).json()["hits"] == []

    def test_content_length_short_circuits_before_reading(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = TestClient(_build_app("rest", max_upload_size=10))

        reads: list[int] = []
        original = starlette.datastructures.UploadFile.read

        async def tracked(self: Any, *args: Any, **kwargs: Any) -> bytes:
            reads.append(1)
            return await original(self, *args, **kwargs)

        monkeypatch.setattr(starlette.datastructures.UploadFile, "read", tracked)

        rejected = client.post(
            "/files",
            files={"file": ("big.bin", b"x" * 1000, "application/octet-stream")},
        )

        assert rejected.status_code == 422
        assert rejected.headers[ERROR_CODE_HEADER] == "upload_too_large"
        assert reads == []

    def test_none_disables_the_cap(self) -> None:
        client = TestClient(_build_app("rest", max_upload_size=None))

        # Larger than the 64 MiB default — only ``None`` lets it through.
        data = b"x" * (DEFAULT_MAX_UPLOAD_SIZE + 1)
        uploaded = client.post(
            "/files",
            files={"file": ("huge.bin", data, "application/octet-stream")},
        )

        assert uploaded.status_code == 201

    def test_streaming_cap_applies_without_content_length(self) -> None:
        # A chunked transfer carries no Content-Length, so only the streamed
        # read loop can enforce the cap.
        app = _build_app("rest", max_upload_size=10)
        client = TestClient(app)

        boundary = "capboundary"
        body = (
            f"--{boundary}\r\n"
            'Content-Disposition: form-data; name="file"; filename="big.bin"\r\n'
            "Content-Type: application/octet-stream\r\n\r\n"
        ).encode() + b"x" * 1000 + f"\r\n--{boundary}--\r\n".encode()

        rejected = client.post(
            "/files",
            content=iter([body]),
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        )

        assert rejected.status_code == 422
        assert rejected.headers[ERROR_CODE_HEADER] == "upload_too_large"
