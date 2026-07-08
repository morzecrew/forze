"""Tests for generated storage routes (forze_fastapi.routes.attach_storage_routes)."""

from __future__ import annotations

import inspect
from typing import Any

import pytest

pytest.importorskip("fastapi")

import starlette.datastructures
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from forze.application.contracts.storage import (
    StorageCommandDepKey,
    StorageSpec,
    StorageUploadSessionDepKey,
)
from forze.application.execution.operations import OperationRegistry
from forze.base.exceptions import CoreException, exc
from forze_fastapi.exceptions import ERROR_CODE_HEADER, register_exception_handlers
from forze_fastapi.routes import DEFAULT_MAX_UPLOAD_SIZE, attach_storage_routes
from forze_kits.aggregates.storage import StorageKernelOp, build_storage_registry
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters import MockStorageAdapter
from tests.support.execution_context import context_from_modules

# ----------------------- #

_UNSET: Any = object()


def _partial_registry(spec: StorageSpec, ops):
    """A registry holding only *ops* (to exercise capability-aware skip).

    Built from the full kit registry, narrowed to the requested operation keys —
    so the bindings for the omitted ops have no registered operation and are
    skipped (not errored) by the capability-aware attacher.
    """

    full = build_storage_registry(spec)
    ns = spec.default_namespace
    wanted = {ns.key(op) for op in ops}

    handlers = {key: fac for key, fac in full._handlers.items() if key in wanted}
    descriptors = {
        key: desc for key, desc in full.get_descriptors().items() if key in wanted
    }

    return OperationRegistry(handlers=handlers, descriptors=descriptors)


def _build_app(
    style="rest",
    *,
    state: MockState | None = None,
    include=None,
    registry_ops=None,
    max_upload_size: int | None = _UNSET,
    stream: bool = _UNSET,
    max_range_bytes: int = _UNSET,
    ctx_dep=None,
) -> FastAPI:
    spec = StorageSpec(name="files")
    state = state or MockState()

    kwargs = {} if max_upload_size is _UNSET else {"max_upload_size": max_upload_size}
    if stream is not _UNSET:
        kwargs["stream"] = stream
    if max_range_bytes is not _UNSET:
        kwargs["max_range_bytes"] = max_range_bytes

    if registry_ops is None:
        registry = build_storage_registry(spec).freeze()
    else:
        registry = _partial_registry(spec, registry_ops).freeze()

    if ctx_dep is None:

        def ctx_dep():
            return context_from_modules(MockDepsModule(state=state))

    router = APIRouter(prefix="/files")
    attach_storage_routes(
        router,
        registry=registry,
        ns=spec.default_namespace,
        ctx_dep=ctx_dep,
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
            "/files/presign_download",
            "/files/presign_upload",
            "/files/begin_upload",
            "/files/presign_part",
            "/files/list_parts",
            "/files/complete_upload",
            "/files/abort_upload",
        }
        assert set(paths["/files/upload"]) == {"post"}
        assert set(paths["/files/download/{key}"]) == {"get"}
        assert set(paths["/files/delete/{key}"]) == {"delete"}
        assert set(paths["/files/begin_upload"]) == {"post"}
        assert set(paths["/files/presign_upload"]) == {"post"}

    @pytest.mark.parametrize("style", ["rest", "rpc"])
    def test_operation_ids_are_registry_keys_verbatim(self, style: str) -> None:
        # head / download_stream / download_range have no standalone route — they are consumed
        # internally by the streaming download endpoint — so they carry no route operation_id.
        internal = {
            StorageKernelOp.HEAD,
            StorageKernelOp.DOWNLOAD_STREAM,
            StorageKernelOp.DOWNLOAD_RANGE,
        }
        expected = {
            f"files.{op.value}" for op in StorageKernelOp if op not in internal
        }

        assert _operation_ids(_build_app(style)) == expected

    def test_include_narrows_to_subset(self) -> None:
        app = _build_app("rest", include={StorageKernelOp.LIST})

        assert _operation_ids(app) == {"files.list"}

    def test_include_of_unknown_operation_raises(self) -> None:
        with pytest.raises(CoreException, match="Unknown operations"):
            _build_app("rest", include={"nope"})

    def test_rest_presign_and_multipart_paths(self) -> None:
        paths = _build_app("rest").openapi()["paths"]

        for path, verb in {
            "/files/presign/download": "post",
            "/files/presign/upload": "post",
            "/files/uploads": "post",
            "/files/uploads/parts/url": "post",
            "/files/uploads/parts": "post",
            "/files/uploads/complete": "post",
            "/files/uploads/abort": "post",
        }.items():
            assert path in paths, path
            assert verb in paths[path], (path, verb)

    def test_capability_aware_skip_partial_registry(self) -> None:
        # A registry holding only LIST: only its route attaches; the presign /
        # multipart routes are skipped (not registered), not errored.
        app = _build_app("rest", include=None, registry_ops={StorageKernelOp.LIST})

        assert _operation_ids(app) == {"files.list"}


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

        resp = client.get(f"/files/{stored['key']}", headers={"Range": "bytes=2-5"})

        assert resp.status_code == 206
        assert resp.content == b"2345"
        assert resp.headers["content-range"] == "bytes 2-5/10"

    def test_open_ended_range(self) -> None:
        client = TestClient(_build_app("rest"))
        stored = self._upload(client)

        resp = client.get(f"/files/{stored['key']}", headers={"Range": "bytes=7-"})

        assert resp.status_code == 206
        assert resp.content == b"789"
        assert resp.headers["content-range"] == "bytes 7-9/10"

    def test_suffix_range(self) -> None:
        client = TestClient(_build_app("rest"))
        stored = self._upload(client)

        resp = client.get(f"/files/{stored['key']}", headers={"Range": "bytes=-3"})

        assert resp.status_code == 206
        assert resp.content == b"789"
        assert resp.headers["content-range"] == "bytes 7-9/10"

    def test_unsatisfiable_range_returns_416(self) -> None:
        client = TestClient(_build_app("rest"))
        stored = self._upload(client)

        resp = client.get(f"/files/{stored['key']}", headers={"Range": "bytes=99-"})

        assert resp.status_code == 416
        assert resp.headers["content-range"] == "bytes */10"

    def test_malformed_range_is_ignored_and_serves_full_body(self) -> None:
        client = TestClient(_build_app("rest"))
        stored = self._upload(client)

        for bad in ("bytes=abc-5", "items=0-3", "bytes=0-2,5-7", "garbage"):
            resp = client.get(f"/files/{stored['key']}", headers={"Range": bad})

            assert resp.status_code == 200, bad
            assert resp.content == b"0123456789"
            assert "content-range" not in resp.headers

    def test_suffix_range_on_empty_body_returns_416(self) -> None:
        client = TestClient(_build_app("rest"))
        stored = client.post(
            "/files",
            files={"file": ("empty.bin", b"", "application/octet-stream")},
        ).json()

        resp = client.get(f"/files/{stored['key']}", headers={"Range": "bytes=-5"})

        # No bytes to serve: 416 with a valid ``*/0`` header, never ``0--1/0``.
        assert resp.status_code == 416
        assert resp.headers["content-range"] == "bytes */0"

    def test_if_none_match_matching_etag_returns_304(self) -> None:
        client = TestClient(_build_app("rest"))
        stored = self._upload(client)

        full = client.get(f"/files/{stored['key']}")
        etag = full.headers["etag"]

        resp = client.get(f"/files/{stored['key']}", headers={"If-None-Match": etag})

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

    def test_if_none_match_list_with_comma_containing_tag_still_matches(self) -> None:
        # An opaque-tag may contain a comma; a naive split would shred the list
        # and could lose the matching etag. The real etag (a later list element)
        # must still 304, and a weak validator must too.
        client = TestClient(_build_app("rest"))
        stored = self._upload(client)
        etag = client.get(f"/files/{stored['key']}").headers["etag"]

        matched = client.get(
            f"/files/{stored['key']}",
            headers={"If-None-Match": f'"a,b,c", W/{etag}'},
        )
        assert matched.status_code == 304

    def test_if_none_match_star_returns_304(self) -> None:
        client = TestClient(_build_app("rest"))
        stored = self._upload(client)

        resp = client.get(
            f"/files/{stored['key']}", headers={"If-None-Match": "*"}
        )
        assert resp.status_code == 304

    def test_if_none_match_only_comma_tag_non_matching_returns_200(self) -> None:
        client = TestClient(_build_app("rest"))
        stored = self._upload(client)

        resp = client.get(
            f"/files/{stored['key']}", headers={"If-None-Match": '"x,y", "z"'}
        )
        assert resp.status_code == 200
        assert resp.content == b"0123456789"

    def test_if_none_match_weak_prefix_only_strips_leading_w_slash(self) -> None:
        # ``removeprefix("W/")`` strips a single literal ``W/`` (weak validator)
        # but must NOT over-strip a ``WW/`` prefix the way ``lstrip("W/")`` did
        # (that stripped every leading W/'/' char). A ``WW/"etag"`` validator is
        # not the object's strong etag, so it does not 304.
        client = TestClient(_build_app("rest"))
        stored = self._upload(client)

        full = client.get(f"/files/{stored['key']}")
        etag = full.headers["etag"]

        # A genuine weak validator (single W/) over the strong etag still 304s.
        weak = client.get(
            f"/files/{stored['key']}", headers={"If-None-Match": f"W/{etag}"}
        )
        assert weak.status_code == 304

        # Double-W must not be stripped down to the bare etag → no 304.
        double = client.get(
            f"/files/{stored['key']}", headers={"If-None-Match": f"WW/{etag}"}
        )
        assert double.status_code == 200
        assert double.content == b"0123456789"


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
            (
                f"--{boundary}\r\n"
                'Content-Disposition: form-data; name="file"; filename="big.bin"\r\n'
                "Content-Type: application/octet-stream\r\n\r\n"
            ).encode()
            + b"x" * 1000
            + f"\r\n--{boundary}--\r\n".encode()
        )

        rejected = client.post(
            "/files",
            content=iter([body]),
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        )

        assert rejected.status_code == 422
        assert rejected.headers[ERROR_CODE_HEADER] == "upload_too_large"


# ....................... #


class TestStoragePresignRoutes:
    """Presigned-URL endpoints: the url rides the response body, never the log."""

    def test_presign_download_returns_get_url_in_body(self) -> None:
        client = TestClient(_build_app("rest"))

        resp = client.post(
            "/files/presign/download",
            json={"key": "docs/report.pdf", "expires_in": 300},
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["method"] == "GET"
        assert body["url"]  # the credential the client needs is in the body
        assert "docs/report.pdf" in body["url"]

    def test_presign_upload_returns_put_url_and_headers(self) -> None:
        client = TestClient(_build_app("rest"))

        resp = client.post(
            "/files/presign/upload",
            json={
                "key": "docs/new.pdf",
                "expires_in": 300,
                "content_type": "application/pdf",
            },
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["method"] == "PUT"
        assert body["url"]
        assert body["headers"]["Content-Type"] == "application/pdf"

    def test_presigned_url_is_not_written_to_the_access_log(self) -> None:
        import io
        import json

        from forze.base.logging import configure_logging
        from forze_fastapi._logging import ForzeFastAPILogger
        from forze_fastapi.middlewares.logging import LoggingMiddleware

        buf = io.StringIO()
        configure_logging(
            level="info",
            logger_names=[str(ForzeFastAPILogger.ACCESS)],
            stream=buf,
            render_mode="json",
        )

        app = _build_app("rest")
        app.add_middleware(LoggingMiddleware)
        client = TestClient(app)

        resp = client.post(
            "/files/presign/download",
            json={"key": "docs/secret.pdf", "expires_in": 300},
        )

        url = resp.json()["url"]
        assert url

        records = [
            json.loads(line)
            for line in buf.getvalue().strip().split("\n")
            if line.strip().startswith("{")
        ]
        # The access log logged the request (path/status/duration) but never the
        # response body — so the presigned URL appears in no log line.
        assert records, "expected an access-log record"
        assert all(url not in json.dumps(rec) for rec in records)
        # Sanity: the request path was logged (so we know logging ran) — the URL
        # is carried under the ``http`` extra, never the response body.
        assert any("/files/presign/download" in json.dumps(rec) for rec in records)


# ....................... #


class TestStorageMultipartRoutes:
    """The resumable multipart flow driven over HTTP (parts deposited out-of-band)."""

    def test_multipart_flow_over_http(self) -> None:
        state = MockState()
        client = TestClient(_build_app("rest", state=state))

        # begin -> session handle (201)
        begun = client.post(
            "/files/uploads",
            json={"key": "big/blob.bin", "content_type": "application/octet-stream"},
        )
        assert begun.status_code == 201
        session = begun.json()
        assert session["key"] == "big/blob.bin"
        assert session["upload_id"]

        # request a presigned URL per part
        for n in (1, 2, 3):
            part_url = client.post(
                "/files/uploads/parts/url",
                json={"session": session, "part_number": n, "expires_in": 300},
            )
            assert part_url.status_code == 200
            assert part_url.json()["method"] == "PUT"

        # the client PUTs the part bytes directly to the presigned URLs; here we
        # deposit them out-of-band through the mock seam against the shared state.
        adapter = MockStorageAdapter(state=state, bucket="files")
        from forze.application.contracts.storage import UploadSession

        sess = UploadSession(
            key=session["key"],
            upload_id=session["upload_id"],
            bucket=session.get("bucket"),
            content_type=session.get("content_type"),
        )
        for n, data in {1: b"aaaa", 2: b"bbbb", 3: b"cccc"}.items():
            adapter.deposit_part(sess, n, data)

        # list_parts over HTTP (resume primitive)
        listed = client.post("/files/uploads/parts", json={"session": session})
        assert listed.status_code == 200
        parts = listed.json()["parts"]
        assert [p["part_number"] for p in parts] == [1, 2, 3]

        # complete over HTTP -> 200 ObjectHead
        completed = client.post(
            "/files/uploads/complete",
            json={"session": session, "parts": parts},
        )
        assert completed.status_code == 200
        head = completed.json()
        assert head["size"] == len(b"aaaabbbbcccc")
        assert head["etag"]

        # the assembled object is now downloadable through the byte route
        downloaded = client.get(f"/files/{session['key']}")
        assert downloaded.status_code == 200
        assert downloaded.content == b"aaaabbbbcccc"

    def test_abort_returns_204(self) -> None:
        client = TestClient(_build_app("rest"))

        session = client.post("/files/uploads", json={"key": "big/blob.bin"}).json()

        aborted = client.post("/files/uploads/abort", json={"session": session})
        assert aborted.status_code == 204


# ....................... #


class _RefusingStoragePort:
    """A storage port that refuses presign/multipart like a client-side-encrypting
    adapter does (the app never sees the bytes, so it cannot encrypt them)."""

    async def presign_upload(self, key, *, expires_in, content_type=None):
        raise exc.precondition(
            "Presigned URLs are unavailable when client-side encryption is enabled.",
        )

    async def begin_upload(self, key, *, content_type=None):
        raise exc.configuration(
            "Multipart upload sessions are unavailable when client-side "
            "encryption is enabled.",
        )

    # Unused on the refusing paths, present for protocol completeness.
    async def upload(self, obj):  # pragma: no cover
        raise NotImplementedError

    async def delete(self, key):  # pragma: no cover
        raise NotImplementedError


def _encrypting_ctx_dep():
    """A ctx whose command + uploads ports refuse, mirroring the encrypting route."""

    def _refuse(_ctx, _spec):
        return _RefusingStoragePort()

    base = MockDepsModule(state=MockState())()
    plain = dict(base.plain_deps)
    plain[StorageCommandDepKey] = _refuse
    plain[StorageUploadSessionDepKey] = _refuse

    from forze.application.execution import Deps
    from tests.support.execution_context import context_from_deps

    deps = Deps.plain(plain)

    def _ctx_dep():
        return context_from_deps(deps)

    return _ctx_dep


class TestStorageEncryptingRouteRefusal:
    """A presign/multipart op on a client-side-encrypting route surfaces an error."""

    def test_presign_upload_refused_propagates_error_status(self) -> None:
        client = TestClient(
            _build_app("rest", ctx_dep=_encrypting_ctx_dep()),
            raise_server_exceptions=False,
        )

        resp = client.post(
            "/files/presign/upload",
            json={"key": "docs/x.pdf", "expires_in": 300},
        )

        # exc.precondition maps to 400 — a clean client error, not a crash.
        assert resp.status_code == 400

    def test_begin_upload_refused_propagates_error_status(self) -> None:
        client = TestClient(
            _build_app("rest", ctx_dep=_encrypting_ctx_dep()),
            raise_server_exceptions=False,
        )

        resp = client.post("/files/uploads", json={"key": "big/blob.bin"})

        # exc.configuration maps to the default 500 (a server-side misconfig); the
        # error propagates cleanly through run_operation to a proper status — no
        # leaked stack, standard JSON body.
        assert resp.status_code == 500
        assert resp.json() == {"detail": "Internal server error"}


class TestStreamingDownload:
    """The default (stream=True) download route: bounded-memory body, backend Range, conditional."""

    @staticmethod
    def _upload(client: TestClient, data: bytes, filename: str = "blob.bin") -> str:
        return client.post(
            "/files",
            files={"file": (filename, data, "application/octet-stream")},
        ).json()["key"]

    def test_full_download_streams_the_body(self) -> None:
        client = TestClient(_build_app("rest"))
        key = self._upload(client, b"0123456789")

        resp = client.get(f"/files/{key}")

        assert resp.status_code == 200
        assert resp.content == b"0123456789"
        assert resp.headers["accept-ranges"] == "bytes"
        assert resp.headers["content-length"] == "10"
        assert "etag" in resp.headers
        assert "blob.bin" in resp.headers["content-disposition"]

    def test_range_returns_206_backend_partial(self) -> None:
        client = TestClient(_build_app("rest"))
        key = self._upload(client, b"0123456789")

        resp = client.get(f"/files/{key}", headers={"Range": "bytes=2-5"})

        assert resp.status_code == 206
        assert resp.content == b"2345"  # end inclusive
        assert resp.headers["content-range"] == "bytes 2-5/10"

    def test_unsatisfiable_range_returns_416(self) -> None:
        client = TestClient(_build_app("rest"))
        key = self._upload(client, b"0123456789")

        resp = client.get(f"/files/{key}", headers={"Range": "bytes=100-"})

        assert resp.status_code == 416
        assert resp.headers["content-range"] == "bytes */10"

    def test_conditional_if_none_match_returns_304(self) -> None:
        client = TestClient(_build_app("rest"))
        key = self._upload(client, b"0123456789")

        etag = client.get(f"/files/{key}").headers["etag"]
        resp = client.get(f"/files/{key}", headers={"If-None-Match": etag})

        assert resp.status_code == 304
        assert resp.content == b""

    def test_range_window_is_capped(self) -> None:
        # A window wider than the cap is served truncated (a valid partial the client re-requests).
        client = TestClient(_build_app("rest", max_range_bytes=4))
        key = self._upload(client, b"0123456789")

        resp = client.get(f"/files/{key}", headers={"Range": "bytes=0-9"})

        assert resp.status_code == 206
        assert resp.content == b"0123"  # capped to 4 bytes
        assert resp.headers["content-range"] == "bytes 0-3/10"

    def test_malformed_range_serves_full_body(self) -> None:
        client = TestClient(_build_app("rest"))
        key = self._upload(client, b"0123456789")

        resp = client.get(f"/files/{key}", headers={"Range": "items=0-4"})

        assert resp.status_code == 200
        assert resp.content == b"0123456789"

    def test_buffered_opt_out_still_serves_the_body(self) -> None:
        # stream=False keeps the fully-buffered download route.
        client = TestClient(_build_app("rest", stream=False))
        key = self._upload(client, b"0123456789")

        resp = client.get(f"/files/{key}")

        assert resp.status_code == 200
        assert resp.content == b"0123456789"
