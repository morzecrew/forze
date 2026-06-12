"""Module/factory wiring for the authn event sink and login lockout (identity plane)."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from tests.support.execution_context import context_from_deps

pytest.importorskip("jwt")
pytest.importorskip("argon2")

pytestmark = pytest.mark.unit

from forze.application.contracts.authn import (
    AuthnDepKey,
    AuthnEvent,
    AuthnEventSink,
    AuthnEventSinkDepKey,
    AuthnSpec,
    PrincipalEligibilityDepKey,
    TokenLifecycleDepKey,
)
from forze.application.contracts.counter import CounterDepKey, CounterPort
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.execution import Deps
from forze.application.integrations.authn import (
    AuthnOrchestrator,
    LockoutConfig,
    LoggingAuthnEventSink,
)
from forze_identity.authn import AuthnDepsModule, AuthnKernelConfig
from forze_identity.authn.application.constants import AuthnResourceName
from forze_identity.authn.execution import ConfigurableLoggingAuthnEventSink
from forze_identity.authn.services import PasswordConfig
from forze_identity.authz.application.constants import AuthzResourceName

# ----------------------- #


def _kernel_full() -> AuthnKernelConfig:
    return AuthnKernelConfig(
        access_token_secret=b"k" * 32,
        refresh_token_pepper=b"p" * 32,
        password=PasswordConfig(time_cost=1, memory_cost=8192, parallelism=1),
        api_key_pepper=b"a" * 32,
        reset_token_pepper=b"r" * 32,
    )


def _document_deps() -> Deps:
    def factory(ctx: object, spec: object) -> MagicMock:
        port = MagicMock()
        port.spec = spec
        return port

    routes = {
        AuthzResourceName.POLICY_PRINCIPALS: factory,
        AuthnResourceName.PASSWORD_ACCOUNTS: factory,
        AuthnResourceName.API_KEY_ACCOUNTS: factory,
        AuthnResourceName.PASSWORD_RESETS: factory,
        AuthnResourceName.TOKEN_SESSIONS: factory,
    }

    return Deps.routed(
        {
            DocumentQueryDepKey: dict(routes),
            DocumentCommandDepKey: dict(routes),
        },
    )


class _RecordingSink(AuthnEventSink):
    def __init__(self) -> None:
        self.events: list[AuthnEvent] = []

    async def record(self, event: AuthnEvent) -> None:
        self.events.append(event)


class _MemoryCounter(CounterPort):
    def __init__(self) -> None:
        self.values: dict[str | None, int] = {}

    async def incr(self, by: int = 1, *, suffix: str | None = None) -> int:
        self.values[suffix] = self.values.get(suffix, 0) + by
        return self.values[suffix]

    async def incr_batch(self, size: int = 2, *, suffix: str | None = None) -> list[int]:
        prev = self.values.get(suffix, 0)
        self.values[suffix] = prev + size
        return list(range(prev + 1, prev + size + 1))

    async def decr(self, by: int = 1, *, suffix: str | None = None) -> int:
        self.values[suffix] = self.values.get(suffix, 0) - by
        return self.values[suffix]

    async def reset(self, value: int = 1, *, suffix: str | None = None) -> int:
        self.values[suffix] = value
        return value


def _counter_deps() -> Deps:
    return Deps.plain({CounterDepKey: lambda ctx, spec: _MemoryCounter()})


# ----------------------- #


class TestEventSinkRegistration:
    def test_shared_sink_registered_for_every_wired_route(self) -> None:
        sink = _RecordingSink()
        deps = AuthnDepsModule(
            kernel=_kernel_full(),
            authn={"main": frozenset({"password"})},
            token_lifecycle={"main", "tl-only"},
            events=lambda ctx, spec: sink,
        )()

        assert deps.exists(AuthnEventSinkDepKey, route="main")
        assert deps.exists(AuthnEventSinkDepKey, route="tl-only")
        assert deps.exists(PrincipalEligibilityDepKey, route="tl-only")

    def test_no_sink_by_default(self) -> None:
        deps = AuthnDepsModule(
            kernel=_kernel_full(),
            authn={"main": frozenset({"password"})},
        )()

        assert not deps.exists(AuthnEventSinkDepKey, route="main")

    def test_configurable_logging_sink_factory(self) -> None:
        factory = ConfigurableLoggingAuthnEventSink()

        sink = factory(MagicMock(), AuthnSpec(name="main"))

        assert isinstance(sink, LoggingAuthnEventSink)

    def test_resolved_orchestrator_and_lifecycle_carry_the_emitter(self) -> None:
        sink = _RecordingSink()
        merged = (
            AuthnDepsModule(
                kernel=_kernel_full(),
                authn={"main": frozenset({"password"})},
                token_lifecycle={"main"},
                events=lambda ctx, spec: sink,
            )()
            .merge(_document_deps())
        )
        ctx = context_from_deps(merged)
        spec = AuthnSpec(name="main", enabled_methods=frozenset({"password"}))

        orchestrator = ctx.deps.provide(AuthnDepKey, route="main")(ctx, spec)
        lifecycle = ctx.deps.provide(TokenLifecycleDepKey, route="main")(ctx, spec)

        assert isinstance(orchestrator, AuthnOrchestrator)
        assert orchestrator.events is not None
        assert orchestrator.events.sink is sink
        assert orchestrator.events.route == "main"
        assert lifecycle.events is not None
        assert lifecycle.events.sink is sink

    def test_without_sink_resolved_ports_have_no_emitter(self) -> None:
        merged = (
            AuthnDepsModule(
                kernel=_kernel_full(),
                authn={"main": frozenset({"password"})},
            )()
            .merge(_document_deps())
        )
        ctx = context_from_deps(merged)
        spec = AuthnSpec(name="main", enabled_methods=frozenset({"password"}))

        orchestrator = ctx.deps.provide(AuthnDepKey, route="main")(ctx, spec)

        assert orchestrator.events is None
        assert orchestrator.lockout is None


class TestLockoutWiring:
    def test_lockout_guard_built_for_password_routes(self) -> None:
        config = LockoutConfig(threshold=3, window=timedelta(minutes=5))
        merged = (
            AuthnDepsModule(
                kernel=_kernel_full(),
                authn={"main": frozenset({"password"})},
                lockout=config,
            )()
            .merge(_document_deps())
            .merge(_counter_deps())
        )
        ctx = context_from_deps(merged)
        spec = AuthnSpec(name="main", enabled_methods=frozenset({"password"}))

        orchestrator = ctx.deps.provide(AuthnDepKey, route="main")(ctx, spec)

        assert orchestrator.lockout is not None
        assert orchestrator.lockout.config is config
        assert isinstance(orchestrator.lockout.counter, _MemoryCounter)

    def test_token_only_routes_skip_the_lockout(self) -> None:
        merged = (
            AuthnDepsModule(
                kernel=_kernel_full(),
                authn={"main": frozenset({"token"})},
                lockout=LockoutConfig(threshold=3),
            )()
            .merge(_document_deps())
            .merge(_counter_deps())
        )
        ctx = context_from_deps(merged)
        spec = AuthnSpec(name="main", enabled_methods=frozenset({"token"}))

        orchestrator = ctx.deps.provide(AuthnDepKey, route="main")(ctx, spec)

        assert orchestrator.lockout is None

    def test_no_lockout_by_default(self) -> None:
        merged = (
            AuthnDepsModule(
                kernel=_kernel_full(),
                authn={"main": frozenset({"password"})},
            )()
            .merge(_document_deps())
        )
        ctx = context_from_deps(merged)
        spec = AuthnSpec(name="main", enabled_methods=frozenset({"password"}))

        orchestrator = ctx.deps.provide(AuthnDepKey, route="main")(ctx, spec)

        assert orchestrator.lockout is None
