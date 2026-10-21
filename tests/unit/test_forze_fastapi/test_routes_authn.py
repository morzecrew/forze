"""Tests for generated authn routes (forze_fastapi.routes.attach_authn_routes)."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

import io
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from forze.application.contracts.authn import AuthnSpec
from forze.application.contracts.execution import BeforeStep
from forze.application.contracts.outbox import OutboxSpec
from forze.application.execution.operations import OperationDescriptor
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.application.hooks.authn import AuthnRequired
from forze.base.exceptions import CoreException
from forze.base.logging import configure_logging
from forze.base.primitives import StrKeyNamespace
from forze.base.serialization import PydanticModelCodec
from forze_fastapi.exceptions import ERROR_CODE_HEADER, register_exception_handlers
from forze_fastapi.middlewares import (
    InvocationMetadataMiddleware,
    LoggingMiddleware,
    SecurityContextMiddleware,
)
from forze_fastapi.routes import attach_authn_routes
from forze_fastapi.security import AuthnRequirement, CookieCsrf, HeaderTokenAuthn
from forze_kits.aggregates.authn import (
    AuthnKernelOp,
    AuthnLoginRequestDTO,
    AuthnPasswordResetRequestedPayload,
    AuthnTokenResponseDTO,
    build_authn_registry,
)
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters.identity import seed_password_account
from tests.support.execution_context import context_from_modules

# ----------------------- #

AUTHN_SPEC = AuthnSpec(name="main", enabled_methods=frozenset({"password", "token"}))

_NS = AUTHN_SPEC.default_namespace

_EXPECTED_PATHS = {
    "/auth/login",
    "/auth/refresh",
    "/auth/logout",
    "/auth/change-password",
    "/auth/password-reset/request",
    "/auth/password-reset/confirm",
    "/auth/api-keys",
    "/auth/api-keys/{id}",
}

# Acts on another principal: registered only behind the app's ``admin_guards``.
_ADMIN_OPS = frozenset(
    {
        AuthnKernelOp.DEACTIVATE_PRINCIPAL,
        AuthnKernelOp.REVOKE_PRINCIPAL_API_KEY,
        AuthnKernelOp.LIST_PRINCIPAL_API_KEYS,
    }
)

# Auth-flow action routes are all POST; the self-service API-key collection uses
# resource verbs (POST create + GET list on the collection, DELETE on the item).
_API_KEY_METHODS = {
    "/auth/api-keys": {"post", "get"},
    "/auth/api-keys/{id}": {"delete"},
}


# ....................... #


def _build_app(
    *,
    state: MockState | None = None,
    include: Any = None,
    registry: FrozenOperationRegistry | None = None,
) -> FastAPI:
    """Plain app: routes + exception handlers, no boundary middlewares."""

    state = state or MockState()
    ctx = context_from_modules(MockDepsModule(state=state))

    router = APIRouter(prefix="/auth")
    attach_authn_routes(
        router,
        registry=registry or build_authn_registry(AUTHN_SPEC).freeze(),
        ns=_NS,
        ctx_dep=lambda: ctx,
        include=include,
    )

    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)

    return app


# ....................... #


def _build_secured_app(state: MockState, *, access_log: bool = False) -> FastAPI:
    """Full ASGI stack: routes behind the forze boundary middlewares."""

    ctx = context_from_modules(MockDepsModule(state=state))

    router = APIRouter(prefix="/auth")
    attach_authn_routes(
        router,
        registry=build_authn_registry(AUTHN_SPEC).freeze(),
        ns=_NS,
        ctx_dep=lambda: ctx,
    )

    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)

    app.add_middleware(
        SecurityContextMiddleware,  # type: ignore[arg-type]
        ctx_dep=lambda: ctx,
        authn=AuthnRequirement(
            ingress=(HeaderTokenAuthn(authn_spec=AUTHN_SPEC, header_name="Authorization"),),
        ),
        when_multiple_credentials="first_in_order",
        # The routes that authenticate from their body, reachable without a credential.
        anonymous_paths={
            "/auth/login",
            "/auth/refresh",
            "/auth/password-reset/request",
            "/auth/password-reset/confirm",
        },
    )
    app.add_middleware(
        InvocationMetadataMiddleware,  # type: ignore[arg-type]
        ctx_dep=lambda: ctx,
    )

    if access_log:
        app.add_middleware(LoggingMiddleware)  # type: ignore[arg-type]

    return app


# ....................... #


def _partial_registry(ns: StrKeyNamespace) -> FrozenOperationRegistry:
    """A registry holding only ``password_login`` (capability-awareness probe)."""

    async def _noop(args: Any) -> Any:
        _ = args
        return None

    reg = OperationRegistry(
        handlers={ns.key(AuthnKernelOp.PASSWORD_LOGIN): lambda _ctx: _noop},
    )
    return reg.set_descriptors(
        {
            AuthnKernelOp.PASSWORD_LOGIN: OperationDescriptor(
                input_type=AuthnLoginRequestDTO,
                output_type=AuthnTokenResponseDTO,
            ),
        },
        namespace=ns,
    ).freeze()


async def _allow(args: Any) -> None:
    _ = args


class _AdminCheck:
    """An app's own authorization step, declaring the permission it enforces (and here
    allowing everyone)."""

    def __call__(self, ctx: object) -> Any:
        return _allow

    def permission_keys(self) -> tuple[str, ...]:
        return ("principals:admin",)


def _guarded(*, allow_anonymous: bool = False) -> FrozenOperationRegistry:
    """The authn registry with its admin operations, behind an app's own step (and
    ``AuthnRequired`` unless *allow_anonymous*)."""

    app_step = BeforeStep(id="app.admin", factory=_AdminCheck())
    guards = (app_step,) if allow_anonymous else (AuthnRequired().to_step(), app_step)

    return build_authn_registry(AUTHN_SPEC, admin_guards=guards).freeze()


def _operation_ids(app: FastAPI) -> set[str]:
    return {
        operation["operationId"]
        for methods in app.openapi()["paths"].values()
        for operation in methods.values()
    }


def _seed(state: MockState, *, password: str = "pw-1") -> None:
    seed_password_account(
        state,
        login="alice",
        password=password,
        principal_id=uuid4(),
    )


def _login(client: TestClient, *, password: str = "pw-1") -> dict[str, Any]:
    response = client.post(
        "/auth/login",
        json={"login": "alice", "password": password},
    )
    assert response.status_code == 200
    body: dict[str, Any] = response.json()
    return body


# ....................... #


class TestAuthnRouteSurface:
    def test_auth_flows_are_post_api_keys_are_resource_verbs(self) -> None:
        paths = _build_app().openapi()["paths"]

        assert set(paths) == _EXPECTED_PATHS

        for path, methods in paths.items():
            expected = _API_KEY_METHODS.get(path, {"post"})
            assert set(methods) == expected, path

    def test_operation_ids_are_registry_keys_verbatim(self) -> None:
        assert _operation_ids(_build_app()) == {
            f"main.{op.value}" for op in AuthnKernelOp if op not in _ADMIN_OPS
        }
        assert _operation_ids(_build_app(registry=_guarded())) == {
            f"main.{op.value}" for op in AuthnKernelOp
        }

    @pytest.mark.parametrize(
        ("method", "path", "body"),
        [
            ("POST", "/auth/deactivate", {"principal_id": str(uuid4())}),
            ("DELETE", f"/auth/admin/api-keys/{uuid4()}", None),
            ("GET", f"/auth/admin/principals/{uuid4()}/api-keys", None),
        ],
        ids=["deactivate", "admin-revoke", "admin-list"],
    )
    def test_admin_routes_exist_only_behind_the_apps_guards(
        self, method: str, path: str, body: Any
    ) -> None:
        unguarded = TestClient(_build_app())
        guarded = TestClient(_build_app(registry=_guarded()))

        assert unguarded.request(method, path, json=body).status_code in (404, 405)
        assert guarded.request(method, path, json=body).status_code == 401

    def test_request_and_response_schemas_come_from_descriptors(self) -> None:
        spec = _build_app().openapi()

        def body_ref(path: str) -> str:
            schema: dict[str, Any] = spec["paths"][path]["post"]["requestBody"]["content"][
                "application/json"
            ]["schema"]
            ref: str = schema["$ref"]
            return ref

        def ok_ref(path: str) -> str:
            schema: dict[str, Any] = spec["paths"][path]["post"]["responses"]["200"]["content"][
                "application/json"
            ]["schema"]
            ref: str = schema["$ref"]
            return ref

        assert body_ref("/auth/login").endswith("/AuthnLoginRequestDTO")
        assert ok_ref("/auth/login").endswith("/AuthnTokenResponseDTO")

        assert body_ref("/auth/refresh").endswith("/AuthnRefreshRequestDTO")
        assert ok_ref("/auth/refresh").endswith("/AuthnTokenResponseDTO")

        assert body_ref("/auth/change-password").endswith("/AuthnChangePasswordRequestDTO")

        # Password reset: request answers 202 with the uniform ack DTO; confirm
        # is a void 204.
        assert body_ref("/auth/password-reset/request").endswith("/AuthnRequestPasswordResetDTO")
        request_reset = spec["paths"]["/auth/password-reset/request"]["post"]
        ack_ref: str = request_reset["responses"]["202"]["content"]["application/json"]["schema"][
            "$ref"
        ]
        assert ack_ref.endswith("/AuthnPasswordResetAckDTO")

        assert body_ref("/auth/password-reset/confirm").endswith("/AuthnResetPasswordDTO")

        # Logout takes no payload at all; the void operations answer 204.
        logout = spec["paths"]["/auth/logout"]["post"]
        assert "requestBody" not in logout
        for path in (
            "/auth/logout",
            "/auth/change-password",
            "/auth/password-reset/confirm",
        ):
            assert "204" in spec["paths"][path]["post"]["responses"]

        guarded = _build_app(registry=_guarded()).openapi()["paths"]["/auth/deactivate"]["post"]
        assert guarded["requestBody"]["content"]["application/json"]["schema"]["$ref"].endswith(
            "/DeactivatePrincipalRequestDTO"
        )
        assert "204" in guarded["responses"]

    def test_include_narrows_to_subset(self) -> None:
        app = _build_app(
            include={AuthnKernelOp.PASSWORD_LOGIN, AuthnKernelOp.REFRESH_TOKENS},
        )

        assert _operation_ids(app) == {"main.password_login", "main.refresh_tokens"}

    def test_include_of_unknown_operation_raises(self) -> None:
        with pytest.raises(CoreException, match="Unknown operations"):
            _build_app(include={"nope"})

    def test_include_of_unregistered_operation_raises(self) -> None:
        with pytest.raises(CoreException, match="is not registered"):
            _build_app(
                registry=_partial_registry(_NS),
                include={AuthnKernelOp.LOGOUT},
            )

    def test_namespace_defaults_to_the_registrys_authn_namespace(self) -> None:
        # build_authn_registry(spec) registers under spec.default_namespace; with
        # neither ns= nor resource=, that is the namespace the routes dispatch to.
        router = APIRouter(prefix="/auth")
        attach_authn_routes(
            router,
            registry=build_authn_registry(AUTHN_SPEC).freeze(),
            ctx_dep=lambda: context_from_modules(MockDepsModule(state=MockState())),
        )
        app = FastAPI()
        app.include_router(router)

        assert _operation_ids(app) == _operation_ids(_build_app())

    def test_namespace_is_not_guessed_between_two_authn_namespaces(self) -> None:
        registry = OperationRegistry.merge(
            build_authn_registry(AUTHN_SPEC),
            build_authn_registry(AUTHN_SPEC, ns=StrKeyNamespace(prefix="other")),
        ).freeze()

        with pytest.raises(CoreException, match="main, other"):
            attach_authn_routes(APIRouter(), registry=registry, ctx_dep=lambda: None)  # type: ignore[arg-type, return-value]

    def test_namespace_inference_without_authn_operations_raises(self) -> None:
        with pytest.raises(CoreException, match="refresh_tokens: none"):
            attach_authn_routes(
                APIRouter(),
                registry=OperationRegistry().freeze(),
                ctx_dep=lambda: None,  # type: ignore[arg-type, return-value]
            )

    def test_a_foreign_op_named_like_a_kernel_op_is_not_an_authn_namespace(self) -> None:
        foreign = OperationRegistry(handlers={"billing.logout": lambda _ctx: _allow})

        with pytest.raises(CoreException, match="refresh_tokens: none"):
            attach_authn_routes(
                APIRouter(),
                registry=foreign.freeze(),
                ctx_dep=lambda: None,  # type: ignore[arg-type, return-value]
            )

        # Beside a real authn registry it is no rival: the authn namespace is inferred.
        router = APIRouter(prefix="/auth")
        attach_authn_routes(
            router,
            registry=OperationRegistry.merge(build_authn_registry(AUTHN_SPEC), foreign).freeze(),
            ctx_dep=lambda: context_from_modules(MockDepsModule(state=MockState())),
        )
        app = FastAPI()
        app.include_router(router)

        assert _operation_ids(app) == _operation_ids(_build_app())

    def test_a_custom_separator_is_never_inferred(self) -> None:
        registry = build_authn_registry(
            AUTHN_SPEC, ns=StrKeyNamespace(prefix="main", sep=":")
        ).freeze()

        with pytest.raises(CoreException, match="refresh_tokens: none"):
            attach_authn_routes(APIRouter(), registry=registry, ctx_dep=lambda: None)  # type: ignore[arg-type, return-value]

    def test_unregistered_operations_are_skipped(self) -> None:
        app = _build_app(registry=_partial_registry(_NS))

        assert _operation_ids(app) == {"main.password_login"}


# ....................... #


class TestAuthnFlows:
    """Operation flows against pure MockDepsModule, no boundary middlewares."""

    def test_login_returns_token_pair(self) -> None:
        state = MockState()
        _seed(state)
        client = TestClient(_build_app(state=state))

        body = _login(client)

        assert body["access_token"].startswith("mock_access_")
        assert body["refresh_token"].startswith("mock_refresh_")
        assert body["access_token_type"] == "Bearer"
        assert body["access_expires_in"] is not None

    def test_login_with_wrong_password_is_401(self) -> None:
        state = MockState()
        _seed(state)
        client = TestClient(_build_app(state=state))

        response = client.post(
            "/auth/login",
            json={"login": "alice", "password": "nope"},
        )

        assert response.status_code == 401

    def test_login_payload_is_validated(self) -> None:
        client = TestClient(_build_app())

        response = client.post("/auth/login", json={"login": "alice"})

        assert response.status_code == 422

    def test_refresh_rotates_and_old_token_is_single_use(self) -> None:
        state = MockState()
        _seed(state)
        client = TestClient(_build_app(state=state))
        first = _login(client)

        second = client.post(
            "/auth/refresh",
            json={"refresh_token": first["refresh_token"]},
        )

        assert second.status_code == 200
        assert second.json()["access_token"] != first["access_token"]
        assert second.json()["refresh_token"] != first["refresh_token"]

        # Reuse of the rotated refresh token is rejected (family revocation).
        reuse = client.post(
            "/auth/refresh",
            json={"refresh_token": first["refresh_token"]},
        )
        assert reuse.status_code == 401

    def test_logout_without_identity_is_401(self) -> None:
        # No SecurityContextMiddleware here, so no identity is ever bound — the
        # handler guards itself.
        client = TestClient(_build_app())

        response = client.post("/auth/logout")

        assert response.status_code == 401
        assert response.headers.get(ERROR_CODE_HEADER) == "auth_required"

    def test_change_password_without_identity_is_401(self) -> None:
        client = TestClient(_build_app())

        response = client.post(
            "/auth/change-password",
            json={"current_password": "pw-1", "new_password": "pw-2"},
        )

        assert response.status_code == 401
        assert response.headers.get(ERROR_CODE_HEADER) == "auth_required"

    def test_deactivate_refuses_anyone_anonymous_behind_any_guard(self) -> None:
        client = TestClient(_build_app(registry=_guarded(allow_anonymous=True)))

        response = client.post("/auth/deactivate", json={"principal_id": str(uuid4())})

        assert response.status_code == 401


# ....................... #


class TestAuthnRoutesThroughMiddlewares:
    """Full ASGI stack: login is reachable without a bearer token, and the
    token it returns authenticates subsequent guarded calls."""

    def test_login_token_guards_change_password_and_logout(self) -> None:
        state = MockState()
        _seed(state)
        client = TestClient(_build_secured_app(state))

        # 1. Login without any Authorization header — reachable by design.
        first = _login(client)

        # 2. Guarded call without a token is a 401 from the handler guard.
        denied = client.post(
            "/auth/change-password",
            json={"current_password": "pw-1", "new_password": "pw-2"},
        )
        assert denied.status_code == 401

        # 3. The access token from login authenticates the guarded call.
        changed = client.post(
            "/auth/change-password",
            json={"current_password": "pw-1", "new_password": "pw-2"},
            headers={"Authorization": f"Bearer {first['access_token']}"},
        )
        assert changed.status_code == 204

        # 4. Change-password revoked every session: the old token is dead at
        #    the boundary (the middleware rejects it before the route runs).
        stale = client.post(
            "/auth/logout",
            headers={"Authorization": f"Bearer {first['access_token']}"},
        )
        assert stale.status_code == 401

        # 5. Old password no longer logs in; the new one does.
        relogin_old = client.post(
            "/auth/login",
            json={"login": "alice", "password": "pw-1"},
        )
        assert relogin_old.status_code == 401

        fresh = _login(client, password="pw-2")

        # 6. Logout with the fresh token revokes it; reuse is rejected.
        out = client.post(
            "/auth/logout",
            headers={"Authorization": f"Bearer {fresh['access_token']}"},
        )
        assert out.status_code == 204

        again = client.post(
            "/auth/logout",
            headers={"Authorization": f"Bearer {fresh['access_token']}"},
        )
        assert again.status_code == 401

    def test_access_log_never_carries_token_material(self) -> None:
        buf = io.StringIO()
        configure_logging(
            level="info",
            logger_names=["fastapi.access"],
            stream=buf,
            render_mode="json",
        )

        state = MockState()
        _seed(state)
        client = TestClient(_build_secured_app(state, access_log=True))

        body = _login(client)

        logged = buf.getvalue()
        assert "Processed request" in logged
        assert body["access_token"] not in logged
        assert body["refresh_token"] not in logged
        assert "pw-1" not in logged


# ....................... #


RESET_EVENTS = OutboxSpec(
    name="authn_events",
    codec=PydanticModelCodec(AuthnPasswordResetRequestedPayload),
)


def _build_reset_app(state: MockState) -> FastAPI:
    """Plain app whose registry stages reset events on the mock outbox."""

    return _build_app(
        state=state,
        registry=build_authn_registry(AUTHN_SPEC, reset_events=RESET_EVENTS).freeze(),
    )


def _staged_reset_rows(state: MockState) -> list[Any]:
    return list(state.outbox_rows.get(str(RESET_EVENTS.name), []))


class TestPasswordResetRoutes:
    """Reset flows against pure MockDepsModule: uniform 202, outbox delivery
    seam, and the full request → confirm → re-login round trip."""

    def test_request_returns_uniform_202_for_known_and_unknown_login(self) -> None:
        state = MockState()
        _seed(state)
        client = TestClient(_build_reset_app(state))

        known = client.post(
            "/auth/password-reset/request",
            json={"login": "alice"},
        )
        unknown = client.post(
            "/auth/password-reset/request",
            json={"login": "nobody"},
        )

        # Identical status and body — the response neither confirms nor denies
        # that an account exists.
        assert known.status_code == 202
        assert unknown.status_code == 202
        assert known.json() == unknown.json()

    def test_request_response_never_carries_the_token(self) -> None:
        state = MockState()
        _seed(state)
        client = TestClient(_build_reset_app(state))

        response = client.post(
            "/auth/password-reset/request",
            json={"login": "alice"},
        )

        rows = _staged_reset_rows(state)
        assert len(rows) == 1
        token = rows[0].payload["token"]

        assert token
        assert token not in response.text

    def test_request_for_unknown_login_stages_nothing(self) -> None:
        state = MockState()
        _seed(state)
        client = TestClient(_build_reset_app(state))

        response = client.post(
            "/auth/password-reset/request",
            json={"login": "nobody"},
        )

        assert response.status_code == 202
        assert _staged_reset_rows(state) == []

    def test_confirm_with_garbage_token_is_401(self) -> None:
        state = MockState()
        _seed(state)
        client = TestClient(_build_reset_app(state))

        response = client.post(
            "/auth/password-reset/confirm",
            json={"token": "not-a-token", "new_password": "pw-2"},
        )

        assert response.status_code == 401

    def test_full_reset_flow_revokes_sessions_and_rotates_password(self) -> None:
        state = MockState()
        _seed(state)
        client = TestClient(_build_secured_app(state))

        # The secured app has no outbox-wired registry; rebuild a plain client
        # against the same state for the reset endpoints with outbox staging.
        reset_client = TestClient(_build_reset_app(state))

        # 1. Login, proving the old password works and minting a session.
        first = _login(client)

        # 2. Request a reset; the token reaches the outbox, not the response.
        ack = reset_client.post(
            "/auth/password-reset/request",
            json={"login": "alice"},
        )
        assert ack.status_code == 202
        token = _staged_reset_rows(state)[0].payload["token"]
        assert token not in ack.text

        # 3. Confirm with the out-of-band token.
        confirmed = reset_client.post(
            "/auth/password-reset/confirm",
            json={"token": token, "new_password": "pw-2"},
        )
        assert confirmed.status_code == 204

        # 4. Reset revoked every session: the old access token is dead at the
        #    boundary (the middleware rejects it before the route runs).
        stale = client.post(
            "/auth/logout",
            headers={"Authorization": f"Bearer {first['access_token']}"},
        )
        assert stale.status_code == 401

        # 5. The token is single-use.
        reused = reset_client.post(
            "/auth/password-reset/confirm",
            json={"token": token, "new_password": "pw-3"},
        )
        assert reused.status_code == 401

        # 6. Old password no longer logs in; the new one does.
        relogin_old = client.post(
            "/auth/login",
            json={"login": "alice", "password": "pw-1"},
        )
        assert relogin_old.status_code == 401

        fresh = _login(client, password="pw-2")
        assert fresh["access_token"].startswith("mock_access_")


# ....................... #
# Cookie mode: tokens ride HttpOnly cookies, never the body.

_SAME_ORIGIN = {"origin": "http://testserver"}
_CROSS_SITE = {"origin": "https://evil.example"}

# Every request shape that means "no payload": the refresh token comes from the cookie.
_COOKIE_REFRESH_BODIES: list[dict[str, Any]] = [
    {},
    {"json": {}},
    {"content": b"", "headers": {"content-type": "application/json"}},
]
_COOKIE_REFRESH_BODY_IDS = ["no-body", "empty-object", "empty-json-body"]


def _with_headers(variant: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
    return {**variant, "headers": {**variant.get("headers", {}), **headers}}


def _cookie_login(client: TestClient) -> Any:
    """Cookie-mode login from the app's own origin, as a browser sends it."""

    return client.post(
        "/auth/login", json={"login": "alice", "password": "pw-1"}, headers=_SAME_ORIGIN
    )


class TestCookieMode:
    def _cookie_client(self, state: MockState) -> TestClient:
        from forze_fastapi.security import AuthnCookieCarrier

        ctx = context_from_modules(MockDepsModule(state=state))
        router = APIRouter(prefix="/auth")
        attach_authn_routes(
            router,
            registry=build_authn_registry(AUTHN_SPEC).freeze(),
            ns=_NS,
            ctx_dep=lambda: ctx,
            # secure=False: the TestClient speaks plain http and would drop
            # Secure cookies from its jar — production keeps the default True
            cookies=AuthnCookieCarrier(secure=False, refresh_path="/auth/refresh"),
        )

        app = FastAPI()
        app.include_router(router)
        register_exception_handlers(app)

        return TestClient(app)

    def test_login_sets_cookies_and_strips_the_body(self) -> None:
        state = MockState()
        _seed(state)
        client = self._cookie_client(state)

        response = _cookie_login(client)

        assert response.status_code == 200
        body = response.json()
        assert "access_token" not in body  # stripped: it rides the cookie
        assert "refresh_token" not in body
        assert body["access_token_type"] == "Bearer"  # scheme + lifetime stay
        assert body["access_expires_in"] is not None

        set_cookies = response.headers.get_list("set-cookie")
        access = next(c for c in set_cookies if c.startswith("forze_access="))
        refresh = next(c for c in set_cookies if c.startswith("forze_refresh="))
        assert "HttpOnly" in access and "HttpOnly" in refresh
        assert "Path=/auth/refresh" in refresh  # the long-lived credential is scoped
        assert "Max-Age=" in access  # lifetime from the token response

    def test_refresh_rotates_from_the_cookie_without_a_body(self) -> None:
        state = MockState()
        _seed(state)
        client = self._cookie_client(state)
        _cookie_login(client)

        before = client.cookies.get("forze_refresh", path="/auth/refresh")

        # No body: the cookie carries it, behind the default CSRF gate (same origin).
        response = client.post("/auth/refresh", headers=_SAME_ORIGIN)

        assert response.status_code == 200
        body = response.json()
        assert "access_token" not in body and "refresh_token" not in body

        after = client.cookies.get("forze_refresh", path="/auth/refresh")
        assert after is not None and after != before  # single-use rotation landed

    def test_refresh_without_body_or_cookie_is_401(self) -> None:
        state = MockState()
        _seed(state)
        client = self._cookie_client(state)

        response = client.post("/auth/refresh", headers=_SAME_ORIGIN)

        assert response.status_code == 401

    def test_refresh_still_accepts_an_explicit_body(self) -> None:
        # An API client may keep sending the token in the body even in cookie
        # mode; the cookie is the fallback, not the only path.
        state = MockState()
        _seed(state)
        client = self._cookie_client(state)
        _cookie_login(client)

        token = client.cookies.get("forze_refresh", path="/auth/refresh")
        client.cookies.clear()  # no cookie jar: the body must carry it alone

        response = client.post(
            "/auth/refresh", json={"refresh_token": token}, headers=_SAME_ORIGIN
        )

        assert response.status_code == 200
        assert "refresh_token" not in response.json()  # still stripped to cookies

    @pytest.mark.parametrize("variant", _COOKIE_REFRESH_BODIES, ids=_COOKIE_REFRESH_BODY_IDS)
    def test_refresh_reads_the_cookie_for_an_empty_body(self, variant: dict[str, Any]) -> None:
        # Fetch wrappers and generated clients send `{}` (or an empty JSON body)
        # where they mean "no payload": cookie mode, not a validation error.
        state = MockState()
        _seed(state)
        client = self._cookie_client(state)
        _cookie_login(client)

        before = client.cookies.get("forze_refresh", path="/auth/refresh")

        response = client.post("/auth/refresh", **_with_headers(variant, _SAME_ORIGIN))

        assert response.status_code == 200
        after = client.cookies.get("forze_refresh", path="/auth/refresh")
        assert after is not None and after != before

    def test_refresh_with_empty_object_and_no_cookie_is_401(self) -> None:
        state = MockState()
        _seed(state)
        client = self._cookie_client(state)

        response = client.post("/auth/refresh", json={}, headers=_SAME_ORIGIN)

        assert response.status_code == 401

    def test_refresh_body_token_wins_over_the_cookie(self) -> None:
        state = MockState()
        _seed(state)
        client = self._cookie_client(state)
        _cookie_login(client)

        # A valid cookie rides along, but the body's token is the one used.
        response = client.post(
            "/auth/refresh", json={"refresh_token": "not-a-token"}, headers=_SAME_ORIGIN
        )

        assert response.status_code == 401

    def test_refresh_body_without_the_token_is_still_invalid(self) -> None:
        state = MockState()
        _seed(state)
        client = self._cookie_client(state)
        _cookie_login(client)

        response = client.post("/auth/refresh", json={"token": "x"}, headers=_SAME_ORIGIN)

        assert response.status_code == 422

    def test_logout_propagates_non_auth_failures(self) -> None:
        # Idempotency covers ONLY the no-identity case; any other failure from
        # the logout operation must surface, not silently clear cookies.
        import asyncio
        from unittest.mock import MagicMock

        from fastapi import Request, Response

        from forze.base.exceptions import CoreException, exc
        from forze_fastapi.routes.authn import _cookie_logout_endpoint
        from forze_fastapi.security import AuthnCookieCarrier

        async def runner(_payload: object) -> object:
            raise exc.infrastructure("session store down")

        endpoint = _cookie_logout_endpoint(AuthnCookieCarrier())(runner, None, "logout")
        same_origin = Request(
            {
                "type": "http",
                "method": "POST",
                "headers": [(b"host", b"testserver"), (b"origin", b"http://testserver")],
            }
        )

        with pytest.raises(CoreException, match="session store down"):
            asyncio.run(endpoint(request=same_origin, response=MagicMock(spec=Response)))

    def test_logout_expires_both_cookies(self) -> None:
        state = MockState()
        _seed(state)
        client = self._cookie_client(state)
        _cookie_login(client)

        response = client.post("/auth/logout", headers=_SAME_ORIGIN)

        # Idempotent in cookie mode: even with no bound identity (this plain app
        # runs no security middleware) the dead cookies are cleared, never pinned
        # to the browser by a 401.
        assert response.status_code == 204
        cleared = response.headers.get_list("set-cookie")
        assert any("forze_access=" in c and ("Max-Age=0" in c or "max-age=0" in c) for c in cleared)
        assert any(
            "forze_refresh=" in c and ("Max-Age=0" in c or "max-age=0" in c) for c in cleared
        )

    @pytest.mark.parametrize("headers", [_CROSS_SITE, {}], ids=["cross-site", "no-origin"])
    def test_login_without_origin_proof_is_refused_and_sets_no_cookie(
        self, headers: dict[str, str]
    ) -> None:
        state = MockState()
        _seed(state)
        client = self._cookie_client(state)

        refused = client.post(
            "/auth/login", json={"login": "alice", "password": "pw-1"}, headers=headers
        )

        assert refused.status_code == 403
        assert refused.headers[ERROR_CODE_HEADER] == "csrf_rejected"
        assert not refused.headers.get_list("set-cookie")

    @pytest.mark.parametrize("headers", [_CROSS_SITE, {}], ids=["cross-site", "no-origin"])
    def test_logout_without_origin_proof_is_refused_and_clears_no_cookie(
        self, headers: dict[str, str]
    ) -> None:
        state = MockState()
        _seed(state)
        client = self._cookie_client(state)

        refused = client.post("/auth/logout", headers=headers)

        assert refused.status_code == 403
        assert refused.headers[ERROR_CODE_HEADER] == "csrf_rejected"
        assert not refused.headers.get_list("set-cookie")


class TestRefreshCookieCsrf:
    """The refresh cookie is an ambient credential: /refresh applies the access cookie's
    CSRF gate when it reads it, and ``{}`` takes the same path as a body-less request,
    that check included. The access cookie is scoped to ``/api`` here, so a refresh
    carries the refresh cookie alone and the middleware's own gate never fires."""

    def _client(
        self,
        state: MockState,
        *,
        gates: tuple[CookieCsrf | None, ...] = (CookieCsrf(),),
        middleware: bool = True,
    ) -> TestClient:
        """Cookie-mode app; one access-cookie ingress per entry of *gates* (its ``csrf``)."""

        from forze_fastapi.security import AuthnCookieCarrier, CookieTokenAuthn

        ctx = context_from_modules(MockDepsModule(state=state))
        carrier = AuthnCookieCarrier(
            secure=False, access_path="/api", refresh_path="/auth/refresh"
        )

        router = APIRouter(prefix="/auth")
        attach_authn_routes(
            router,
            registry=build_authn_registry(AUTHN_SPEC).freeze(),
            ns=_NS,
            ctx_dep=lambda: ctx,
            cookies=carrier,
        )

        app = FastAPI()
        app.include_router(router)
        register_exception_handlers(app)

        if middleware:
            app.add_middleware(
                SecurityContextMiddleware,  # type: ignore[arg-type]
                ctx_dep=lambda: ctx,
                authn=AuthnRequirement(
                    ingress=tuple(
                        CookieTokenAuthn(
                            authn_spec=AUTHN_SPEC, cookie_name=carrier.access_cookie, csrf=gate
                        )
                        for gate in gates
                    ),
                ),
                when_multiple_credentials="first_in_order",
                anonymous_paths={"/auth/login", "/auth/refresh"},
            )

        client = TestClient(app)
        login = _cookie_login(client)
        assert login.status_code == 200

        return client

    def _refresh_cookie(self, client: TestClient) -> str | None:
        return client.cookies.get("forze_refresh", path="/auth/refresh")

    @pytest.mark.parametrize("variant", _COOKIE_REFRESH_BODIES, ids=_COOKIE_REFRESH_BODY_IDS)
    def test_cross_site_refresh_is_refused_without_rotation(
        self, variant: dict[str, Any]
    ) -> None:
        state = MockState()
        _seed(state)
        client = self._client(state)
        before = self._refresh_cookie(client)

        refused = client.post("/auth/refresh", **_with_headers(variant, _CROSS_SITE))

        assert refused.status_code == 403
        assert refused.headers[ERROR_CODE_HEADER] == "csrf_rejected"
        assert self._refresh_cookie(client) == before
        # The token was not spent: the same cookie still refreshes from the app's origin.
        assert client.post("/auth/refresh", headers=_SAME_ORIGIN).status_code == 200

    @pytest.mark.parametrize("variant", _COOKIE_REFRESH_BODIES, ids=_COOKIE_REFRESH_BODY_IDS)
    def test_refresh_without_origin_proof_is_refused(self, variant: dict[str, Any]) -> None:
        state = MockState()
        _seed(state)
        client = self._client(state)

        refused = client.post("/auth/refresh", **variant)

        assert refused.status_code == 403
        assert refused.headers[ERROR_CODE_HEADER] == "csrf_rejected"

    @pytest.mark.parametrize("variant", _COOKIE_REFRESH_BODIES, ids=_COOKIE_REFRESH_BODY_IDS)
    def test_same_origin_refresh_rotates(self, variant: dict[str, Any]) -> None:
        state = MockState()
        _seed(state)
        client = self._client(state)
        before = self._refresh_cookie(client)

        response = client.post("/auth/refresh", **_with_headers(variant, _SAME_ORIGIN))

        assert response.status_code == 200
        assert self._refresh_cookie(client) != before

    def test_the_apps_allowed_origins_apply(self) -> None:
        state = MockState()
        _seed(state)
        client = self._client(
            state, gates=(CookieCsrf(allowed_origins={"https://app.example.com"}),)
        )

        allowed = client.post("/auth/refresh", headers={"origin": "https://app.example.com"})

        assert allowed.status_code == 200

    def test_body_token_refresh_is_gated(self) -> None:
        # The response sets the session cookies, so a body token is gated as login is.
        state = MockState()
        _seed(state)
        client = self._client(state)
        token = self._refresh_cookie(client)
        client.cookies.clear()

        refused = client.post("/auth/refresh", json={"refresh_token": token}, headers=_CROSS_SITE)

        assert refused.status_code == 403
        assert refused.headers[ERROR_CODE_HEADER] == "csrf_rejected"
        assert not refused.headers.get_list("set-cookie")

        allowed = client.post("/auth/refresh", json={"refresh_token": token}, headers=_SAME_ORIGIN)

        assert allowed.status_code == 200  # the refused attempt did not spend the token

    def test_declared_opt_out_is_honoured(self) -> None:
        state = MockState()
        _seed(state)
        client = self._client(state, gates=(None,))

        assert client.post("/auth/refresh", headers=_CROSS_SITE).status_code == 200

    @pytest.mark.parametrize(
        "gates", [(CookieCsrf(), None), (None, CookieCsrf())], ids=["gate-first", "gate-last"]
    )
    def test_an_opt_out_never_drops_another_ingress_gate(
        self, gates: tuple[CookieCsrf | None, ...]
    ) -> None:
        # Two ingresses read the access cookie, one opted out: the refresh cookie stays gated.
        state = MockState()
        _seed(state)
        client = self._client(state, gates=gates)

        refused = client.post("/auth/refresh", headers=_CROSS_SITE)

        assert refused.status_code == 403
        assert refused.headers[ERROR_CODE_HEADER] == "csrf_rejected"

    def test_gate_stays_on_without_a_cookie_ingress(self) -> None:
        # No security middleware published a policy: the default gate applies.
        state = MockState()
        _seed(state)
        client = self._client(state, middleware=False)

        refused = client.post("/auth/refresh", headers=_CROSS_SITE)

        assert refused.status_code == 403
        assert refused.headers[ERROR_CODE_HEADER] == "csrf_rejected"
        assert client.post("/auth/refresh", headers=_SAME_ORIGIN).status_code == 200


class TestAuthnCookieCarrier:
    """Direct carrier behavior not reachable through the route flow above."""

    def _carrier(self, **kwargs: object):  # type: ignore[no-untyped-def]
        from forze_fastapi.security import AuthnCookieCarrier

        return AuthnCookieCarrier(**kwargs)  # type: ignore[arg-type]

    def test_samesite_none_requires_secure(self) -> None:
        from forze.base.exceptions import CoreException

        with pytest.raises(CoreException, match="SameSite=None"):
            self._carrier(samesite="none", secure=False)

        self._carrier(samesite="none", secure=True)  # the valid pairing builds

    def test_partial_token_response_sets_only_the_present_cookie(self) -> None:
        # A refresh-only response (or access-only) must not emit an empty
        # sibling cookie.
        from fastapi import Response

        from forze_kits.aggregates.authn import AuthnTokenResponseDTO

        response = Response()
        self._carrier(secure=False).apply(
            response, AuthnTokenResponseDTO(refresh_token="r-1", refresh_expires_in=60)
        )
        set_cookies = response.headers.getlist("set-cookie")
        assert any(c.startswith("forze_refresh=") for c in set_cookies)
        assert not any(c.startswith("forze_access=") for c in set_cookies)

        response = Response()
        self._carrier(secure=False).apply(
            response, AuthnTokenResponseDTO(access_token="a-1", access_expires_in=60)
        )
        set_cookies = response.headers.getlist("set-cookie")
        assert any(c.startswith("forze_access=") for c in set_cookies)
        assert not any(c.startswith("forze_refresh=") for c in set_cookies)

    def test_strip_body_tokens_false_keeps_the_body_intact(self) -> None:
        from fastapi import Response

        from forze_kits.aggregates.authn import AuthnTokenResponseDTO

        tokens = AuthnTokenResponseDTO(access_token="a-1", refresh_token="r-1")
        returned = self._carrier(secure=False, strip_body_tokens=False).apply(
            Response(), tokens
        )

        assert returned is tokens  # untouched: the declared opt-out of stripping
