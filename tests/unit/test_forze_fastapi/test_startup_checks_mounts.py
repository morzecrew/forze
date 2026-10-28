"""The startup checks reach mounted applications, and judge them as the gates match.

# covers: forze_fastapi.run_startup_checks, check_bypass_paths, check_websocket_allowlist,
#         check_authn_cookie_paths on mounted apps; bypass_paths / allowed_websocket_paths
#         matched against the mount the way anonymous_paths is

Starlette's ``Mount`` keeps the full path in ``scope["path"]`` and extends ``root_path``,
so a gate on an app mounted at ``/api`` sees ``/api/livez``. The gates now match an entry
as the full path or the path relative to the mount, and the checks walk every mounted
application from the outermost one, holding each one's gates against what they see.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import MagicMock

import pytest

pytest.importorskip("fastapi")

from fastapi import APIRouter, FastAPI, WebSocket
from fastapi.testclient import TestClient
from starlette.routing import Mount, Route

from forze.application.contracts.authn import AuthnSpec
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution import build_runtime
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_fastapi import run_startup_checks, runtime_lifespan
from forze_fastapi.middlewares import (
    InvocationMetadataMiddleware,
    SecurityContextMiddleware,
    check_authn_cookie_paths,
    check_bypass_paths,
    check_websocket_allowlist,
)
from forze_fastapi.middlewares import bypass as bypass_module
from forze_fastapi.middlewares.raw_websocket import websocket_scope_refused
from forze_fastapi.routes import (
    attach_authn_routes,
    attach_document_routes,
    attach_liveness_route,
)
from forze_fastapi.security import AuthnRequirement, HeaderTokenAuthn
from forze_kits.aggregates.authn import build_authn_registry
from forze_kits.aggregates.document import DocumentDTOs, build_document_registry
from forze_mock import MockDepsModule

# ----------------------- #

_TOKEN_SPEC = AuthnSpec(name="auth", enabled_methods=frozenset({"token"}))
_AUTHN_SPEC = AuthnSpec(name="main", enabled_methods=frozenset({"password", "token"}))
_RUNTIME = build_runtime(MockDepsModule())
"""Never started: a gate that resolves the context in front of a probe answers 500."""


class _Note(Document):
    title: str = ""


class _NoteCreate(BaseDTO):
    title: str = ""


class _NoteRead(ReadDocument):
    title: str


# ....................... #


def _gate(app: FastAPI, **kwargs: Any) -> FastAPI:
    """Both gating middlewares, each given *kwargs*."""

    app.add_middleware(
        InvocationMetadataMiddleware,  # type: ignore[arg-type]
        ctx_dep=_RUNTIME.get_context,
        **kwargs,
    )
    app.add_middleware(
        SecurityContextMiddleware,  # type: ignore[arg-type]
        ctx_dep=_RUNTIME.get_context,
        authn=AuthnRequirement(
            ingress=(HeaderTokenAuthn(authn_spec=_TOKEN_SPEC, header_name="Authorization"),)
        ),
        when_multiple_credentials="first_in_order",
        **kwargs,
    )

    return app


def _probes(app: FastAPI) -> FastAPI:
    router = APIRouter()
    attach_liveness_route(router)
    app.include_router(router)

    return app


def _notes(app: FastAPI) -> FastAPI:
    spec = DocumentSpec(
        name="notes",
        read=_NoteRead,
        write=DocumentWriteTypes(domain=_Note, create_cmd=_NoteCreate),
    )
    registry = build_document_registry(
        spec, DocumentDTOs(read=_NoteRead, create=_NoteCreate)
    ).freeze()
    router = APIRouter(prefix="/notes")
    attach_document_routes(
        router,
        registry=registry,
        ns=spec.default_namespace,
        ctx_dep=_RUNTIME.get_context,
        style="rest",
    )
    app.include_router(router)

    return app


def _mounted(api: FastAPI, prefix: str = "/api") -> FastAPI:
    root = FastAPI()
    root.mount(prefix, api)

    return root


def _refused(check: Any, app: FastAPI) -> str:
    with pytest.raises(CoreException) as error:
        check(app)

    return str(error.value)


# ....................... #


class TestBypassOnAMountedApp:
    """The exact layout of the report: the gates sit on an app mounted at ``/api``."""

    def test_the_full_path_bypasses_and_passes_the_check(self) -> None:
        root = _mounted(_gate(_probes(FastAPI()), bypass_paths={"/api/livez"}))

        assert TestClient(root).get("/api/livez").status_code == 200

        # Refused before: the check held /api/livez against the app's own /livez.
        check_bypass_paths(root)

    def test_the_mount_relative_path_bypasses_too(self) -> None:
        root = _mounted(_gate(_probes(FastAPI()), bypass_paths={"/livez"}))

        # Answered 500 before while the check passed: the gate compared the full path.
        assert TestClient(root).get("/api/livez").status_code == 200

        check_bypass_paths(root)

    def test_a_set_matching_nothing_is_refused_from_the_root(self) -> None:
        root = _mounted(_gate(_probes(FastAPI()), bypass_paths={"/v2/livez"}))

        assert "not one of those paths is routed" in _refused(check_bypass_paths, root)

    @pytest.mark.parametrize("entry", ["/notes", "/api/notes"])
    def test_a_generated_route_may_not_be_bypassed(self, entry: str) -> None:
        root = _mounted(_gate(_notes(_probes(FastAPI())), bypass_paths={"/livez", entry}))

        message = _refused(check_bypass_paths, root)

        assert "/api/notes" in message
        assert "unauthenticated" in message

    def test_a_prefix_led_entry_is_read_as_a_full_path_only(self) -> None:
        # Under /api, "/api/livez" names /api/livez and never the route at /api/api/livez:
        # the gate and the check read it the same way.
        api = FastAPI()
        api.get("/api/livez")(lambda: {"ok": True})
        root = _mounted(_gate(api, bypass_paths={"/api/livez"}))

        assert (
            TestClient(root, raise_server_exceptions=False).get("/api/api/livez").status_code == 500
        )
        assert "not one of those paths is routed" in _refused(check_bypass_paths, root)


class TestBypassUnderARootPath:
    """``FastAPI(root_path=...)``: the gate sees the path with it, or without it behind a
    proxy that strips it, so only an entry relative to it bypasses both ways."""

    @pytest.mark.filterwarnings("ignore:The `root_path` parameter is deprecated")
    def test_the_relative_entry_passes(self) -> None:
        check_bypass_paths(_gate(_probes(FastAPI(root_path="/v1")), bypass_paths={"/livez"}))

    @pytest.mark.filterwarnings("ignore:The `root_path` parameter is deprecated")
    def test_the_full_entry_misses_behind_a_stripping_proxy(self) -> None:
        app = _gate(_probes(FastAPI(root_path="/v1")), bypass_paths={"/v1/livez"})

        assert "not one of those paths is routed" in _refused(check_bypass_paths, app)

    @pytest.mark.filterwarnings("ignore:The `root_path` parameter is deprecated")
    def test_a_full_entry_naming_a_generated_route_is_caught(self) -> None:
        # Missed before: without a stripping proxy the gate sees /v1/notes and skips it.
        app = _gate(_notes(_probes(FastAPI(root_path="/v1"))), bypass_paths={"/livez", "/v1/notes"})

        assert "unauthenticated" in _refused(check_bypass_paths, app)


class TestBypassOnTheRootApp:
    """The gates sit on the outer app, in front of everything mounted on it."""

    def test_a_route_of_a_mounted_app_can_be_bypassed(self) -> None:
        root = _gate(FastAPI(), bypass_paths={"/api/livez"})
        root.mount("/api", _probes(FastAPI()))

        assert TestClient(root).get("/api/livez").status_code == 200

        # Refused before: the mounted app's routes were invisible from here.
        check_bypass_paths(root)

    def test_a_generated_route_of_a_mounted_app_is_caught(self) -> None:
        # The superset passed before: /livez matched, and /api/notes was out of sight.
        root = _gate(_probes(FastAPI()), bypass_paths={"/livez", "/api/notes"})
        root.mount("/api", _notes(FastAPI()))

        message = _refused(check_bypass_paths, root)

        assert "/api/notes" in message
        assert "unauthenticated" in message

    def test_a_mounted_router_is_walked_as_part_of_the_app(self) -> None:
        root = _gate(FastAPI(), bypass_paths={"/r/livez"})
        root.router.routes.append(Mount("/r", routes=[Route("/livez", lambda _request: None)]))

        check_bypass_paths(root)

    def test_an_entry_inside_an_uninspectable_mount_is_warned_about(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The forze logger is structlog-backed and writes past caplog, so it is patched.
        logger = MagicMock()
        monkeypatch.setattr(bypass_module, "_logger", logger)

        async def opaque(scope: Any, receive: Any, send: Any) -> None:  # pragma: no cover
            pass

        root = _gate(_probes(FastAPI()), bypass_paths={"/livez", "/raw/livez"})
        root.mount("/raw", opaque)

        check_bypass_paths(root)

        logger.warning.assert_called_once()
        assert "/raw/livez" in str(logger.warning.call_args)

    def test_a_superset_beside_an_uninspectable_mount_stays_quiet(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        logger = MagicMock()
        monkeypatch.setattr(bypass_module, "_logger", logger)

        async def opaque(scope: Any, receive: Any, send: Any) -> None:  # pragma: no cover
            pass

        root = _gate(_probes(FastAPI()), bypass_paths={"/livez", "/metrics"})
        root.mount("/raw", opaque)

        check_bypass_paths(root)

        logger.warning.assert_not_called()


# ....................... #


def _realtime(app: FastAPI) -> FastAPI:
    from forze.application.integrations.realtime import (
        InMemoryMailboxCursors,
        InMemoryRealtimeMailbox,
    )
    from forze_fastapi.realtime import WsConnection, attach_realtime_ws_route

    async def _resolve(connect: Any) -> WsConnection:  # pragma: no cover - unused
        raise NotImplementedError

    router = APIRouter()
    attach_realtime_ws_route(
        router,
        ctx_dep=_RUNTIME.get_context,
        resolve=_resolve,
        mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
        cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
    )
    app.include_router(router)

    return app


class TestWebsocketAllowlistOnAMountedApp:
    @pytest.mark.parametrize("entry", ["/realtime/ws", "/api/realtime/ws"])
    def test_either_reading_is_admitted_and_verified(self, entry: str) -> None:
        root = _mounted(_gate(_realtime(FastAPI()), allowed_websocket_paths={entry}))

        # What the gate on the mounted app sees for a connection to /api/realtime/ws.
        scope = {"type": "websocket", "path": "/api/realtime/ws", "root_path": "/api"}

        assert not websocket_scope_refused(
            scope, allow_raw_websockets=False, allowed_websocket_paths=frozenset({entry})
        )
        check_websocket_allowlist(root)

    def test_a_raw_route_in_a_mounted_app_is_refused(self) -> None:
        api = FastAPI()

        @api.websocket("/ws")
        async def rogue(websocket: WebSocket) -> None:  # pragma: no cover - never runs
            await websocket.accept()

        root = _mounted(_gate(api, allowed_websocket_paths={"/ws"}))

        assert "not a governed realtime route" in _refused(check_websocket_allowlist, root)

    def test_the_root_may_allowlist_a_governed_route_of_a_mounted_app(self) -> None:
        # Refused before as unverifiable; the route is now seen and verified.
        root = _gate(FastAPI(), allowed_websocket_paths={"/api/realtime/ws"})
        root.mount("/api", _realtime(FastAPI()))

        check_websocket_allowlist(root)

    def test_a_path_into_an_uninspectable_mount_is_still_refused(self) -> None:
        async def opaque(scope: Any, receive: Any, send: Any) -> None:  # pragma: no cover
            pass

        root = _gate(FastAPI(), allowed_websocket_paths={"/raw/ws"})
        root.mount("/raw", opaque)

        assert "no websocket route" in _refused(check_websocket_allowlist, root)


# ....................... #


def _cookie_api(anonymous: set[str] | None) -> FastAPI:
    from forze_fastapi.security import AuthnCookieCarrier, CookieTokenAuthn

    router = APIRouter(prefix="/auth")
    attach_authn_routes(
        router,
        registry=build_authn_registry(_AUTHN_SPEC).freeze(),
        ns=_AUTHN_SPEC.default_namespace,
        ctx_dep=_RUNTIME.get_context,
        cookies=AuthnCookieCarrier(secure=False, access_path="/", refresh_path="/api/auth"),
    )
    api = FastAPI()
    api.include_router(router)

    if anonymous is not None:
        api.add_middleware(
            SecurityContextMiddleware,  # type: ignore[arg-type]
            ctx_dep=_RUNTIME.get_context,
            authn=AuthnRequirement(
                ingress=(CookieTokenAuthn(authn_spec=_AUTHN_SPEC, cookie_name="forze_access"),),
            ),
            when_multiple_credentials="first_in_order",
            anonymous_paths=anonymous,
        )

    return api


class TestCookiePathsOnAMountedApp:
    @pytest.mark.parametrize("prefix", ["", "/api"])
    def test_listed_cookie_routes_pass(self, prefix: str) -> None:
        listed = {f"{prefix}/auth/{name}" for name in ("login", "refresh", "logout")}

        check_authn_cookie_paths(_mounted(_cookie_api(listed)))

    def test_a_missing_cookie_route_is_refused_from_the_root(self) -> None:
        root = _mounted(_cookie_api({"/auth/login", "/auth/refresh"}))

        message = _refused(check_authn_cookie_paths, root)

        assert "/api/auth/logout" in message
        assert "anonymous_paths" in message

    def test_a_root_security_middleware_must_list_mounted_cookie_routes(self) -> None:
        root = _mounted(_cookie_api(None))
        root.add_middleware(
            SecurityContextMiddleware,  # type: ignore[arg-type]
            ctx_dep=_RUNTIME.get_context,
            authn=AuthnRequirement(
                ingress=(HeaderTokenAuthn(authn_spec=_TOKEN_SPEC, header_name="Authorization"),)
            ),
            when_multiple_credentials="first_in_order",
            # Relative paths mean nothing on the root: it sees no mount.
            anonymous_paths={"/auth/login", "/auth/refresh", "/auth/logout"},
        )

        assert "anonymous_paths" in _refused(check_authn_cookie_paths, root)

    def test_a_refresh_path_missing_the_mounted_logout_is_refused(self) -> None:
        from forze_fastapi.security import AuthnCookieCarrier

        router = APIRouter(prefix="/auth")
        attach_authn_routes(
            router,
            registry=build_authn_registry(_AUTHN_SPEC).freeze(),
            ns=_AUTHN_SPEC.default_namespace,
            ctx_dep=_RUNTIME.get_context,
            # The route-local prefix: the browser asks for /api/auth/logout.
            cookies=AuthnCookieCarrier(secure=False, access_path="/", refresh_path="/auth"),
        )
        api = FastAPI()
        api.include_router(router)

        assert "/api/auth/logout" in _refused(check_authn_cookie_paths, _mounted(api))


# ....................... #


class TestRunStartupChecks:
    @pytest.mark.parametrize(
        ("api", "expected"),
        [
            (lambda: _gate(_probes(FastAPI()), bypass_paths={"/nope"}), "bypass_paths"),
            (lambda: _gate(FastAPI(), allowed_websocket_paths={"/ws"}), "allowed_websocket"),
            (lambda: _cookie_api({"/auth/login"}), "anonymous_paths"),
        ],
        ids=["bypass", "websocket", "cookie"],
    )
    def test_every_check_runs_on_every_mounted_app(self, api: Any, expected: str) -> None:
        assert expected in _refused(run_startup_checks, _mounted(api()))

    def test_a_sound_layout_passes(self) -> None:
        root = _gate(_probes(FastAPI()), bypass_paths={"/livez"})
        root.mount("/api", _gate(_probes(FastAPI()), bypass_paths={"/livez"}))

        run_startup_checks(root)

    def test_a_hand_written_lifespan_fails_the_boot_with_one_call(self) -> None:
        @asynccontextmanager
        async def lifespan(app: FastAPI) -> Any:
            run_startup_checks(app)

            async with build_runtime(MockDepsModule()).scope():
                yield

        root = FastAPI(lifespan=lifespan)
        root.mount("/api", _gate(_notes(FastAPI()), bypass_paths={"/notes"}))

        with pytest.raises(CoreException), TestClient(root):
            pass  # pragma: no cover - startup is what raises

    def test_runtime_lifespan_checks_the_mounted_apps(self) -> None:
        root = FastAPI(lifespan=runtime_lifespan(build_runtime(MockDepsModule())))
        root.mount("/api", _gate(_notes(FastAPI()), bypass_paths={"/notes"}))

        with pytest.raises(CoreException), TestClient(root):
            pass  # pragma: no cover - startup is what raises
