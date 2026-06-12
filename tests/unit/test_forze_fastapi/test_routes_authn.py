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
from forze.application.contracts.outbox import OutboxSpec
from forze.application.execution.operations import OperationDescriptor
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.base.exceptions import CoreException
from forze.base.logging import configure_logging
from forze.base.primitives import StrKeyNamespace
from forze.base.serialization import PydanticModelCodec
from forze_fastapi._logging import ForzeFastAPILogger
from forze_fastapi.exceptions import ERROR_CODE_HEADER, register_exception_handlers
from forze_fastapi.middlewares import (
    InvocationMetadataMiddleware,
    LoggingMiddleware,
    SecurityContextMiddleware,
)
from forze_fastapi.routes import attach_authn_routes
from forze_fastapi.security import AuthnRequirement, HeaderTokenAuthn
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
    "/auth/deactivate",
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
            ingress=(
                HeaderTokenAuthn(authn_spec=AUTHN_SPEC, header_name="Authorization"),
            ),
        ),
        when_multiple_credentials="first_in_order",
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
    def test_every_operation_is_a_post_on_its_action_path(self) -> None:
        paths = _build_app().openapi()["paths"]

        assert set(paths) == _EXPECTED_PATHS
        assert all(set(methods) == {"post"} for methods in paths.values())

    def test_operation_ids_are_registry_keys_verbatim(self) -> None:
        assert _operation_ids(_build_app()) == {
            f"main.{op.value}" for op in AuthnKernelOp
        }

    def test_request_and_response_schemas_come_from_descriptors(self) -> None:
        spec = _build_app().openapi()

        def body_ref(path: str) -> str:
            schema: dict[str, Any] = spec["paths"][path]["post"]["requestBody"][
                "content"
            ]["application/json"]["schema"]
            ref: str = schema["$ref"]
            return ref

        def ok_ref(path: str) -> str:
            schema: dict[str, Any] = spec["paths"][path]["post"]["responses"]["200"][
                "content"
            ]["application/json"]["schema"]
            ref: str = schema["$ref"]
            return ref

        assert body_ref("/auth/login").endswith("/AuthnLoginRequestDTO")
        assert ok_ref("/auth/login").endswith("/AuthnTokenResponseDTO")

        assert body_ref("/auth/refresh").endswith("/AuthnRefreshRequestDTO")
        assert ok_ref("/auth/refresh").endswith("/AuthnTokenResponseDTO")

        assert body_ref("/auth/change-password").endswith(
            "/AuthnChangePasswordRequestDTO"
        )
        assert body_ref("/auth/deactivate").endswith("/DeactivatePrincipalRequestDTO")

        # Password reset: request answers 202 with the uniform ack DTO; confirm
        # is a void 204.
        assert body_ref("/auth/password-reset/request").endswith(
            "/AuthnRequestPasswordResetDTO"
        )
        request_reset = spec["paths"]["/auth/password-reset/request"]["post"]
        ack_ref: str = request_reset["responses"]["202"]["content"][
            "application/json"
        ]["schema"]["$ref"]
        assert ack_ref.endswith("/AuthnPasswordResetAckDTO")

        assert body_ref("/auth/password-reset/confirm").endswith(
            "/AuthnResetPasswordDTO"
        )

        # Logout takes no payload at all; the void operations answer 204.
        logout = spec["paths"]["/auth/logout"]["post"]
        assert "requestBody" not in logout
        for path in (
            "/auth/logout",
            "/auth/change-password",
            "/auth/password-reset/confirm",
            "/auth/deactivate",
        ):
            assert "204" in spec["paths"][path]["post"]["responses"]

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

    def test_deactivate_attaches_and_ships_unguarded(self) -> None:
        # Documents the default posture: deactivate_principal carries no
        # built-in authn/authz guard — apps must bind AuthnRequired plus an
        # authz before-hook on it (or exclude it) before exposing the router.
        client = TestClient(_build_app())

        response = client.post(
            "/auth/deactivate",
            json={"principal_id": str(uuid4())},
        )

        assert response.status_code == 204


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
            logger_names=[str(ForzeFastAPILogger.ACCESS)],
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
