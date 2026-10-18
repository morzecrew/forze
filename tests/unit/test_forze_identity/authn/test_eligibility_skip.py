"""Skipping the eligibility read after a fast-path admission, on the identity plane's adapters.

The adapters run over mock documents, the mock's journal transactions and the mock states and
epoch stores. The shared battery is the same one Postgres and Redis run; the cases here are the
configuration and credential cases that need the mock's flexibility.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, TypeVar
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.contracts.authn import (
    AuthnSpec,
    PasswordCredentials,
    PrincipalEpochDepKey,
    RevocationStatesDepKey,
)
from forze.application.contracts.authz import AuthzSpec
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.tenancy import TenantResolverDepKey
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze_identity.authn import AuthnDepsModule, AuthnKernelConfig, RevocationStates
from forze_identity.authn.application.specs import (
    api_key_account_spec,
    password_account_spec,
    session_spec,
)
from forze_identity.authn.services import PasswordConfig, PolicyPrincipalEpochHook
from forze_identity.authz import AuthzDepsModule, AuthzKernelConfig
from forze_identity.authz.application.specs import policy_principal_spec
from forze_identity.authz.domain.models.policy_principal import (
    CreatePolicyPrincipalCmd,
    UpdatePolicyPrincipalCmd,
)
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.adapters.revocation import MockPrincipalEpochStore, MockRevocationStates
from forze_mock.execution.factories import ConfigurableMockDocument, mock_journal_txmanager
from tests.support import eligibility_skip_conformance as battery
from tests.support.eligibility_skip_conformance import CountingPrincipalReads
from tests.support.execution_context import context_from_deps

pytest.importorskip("argon2")

pytestmark = pytest.mark.unit

T = TypeVar("T")

# ----------------------- #

ROUTE = "main"
SPEC = AuthnSpec(name=ROUTE, enabled_methods=frozenset({"token", "password"}))
STATES = "authn-revocations"
TX = "default"
CONFIG = RevocationStates(route=STATES, ttl=timedelta(minutes=5))


class _AnyTenant:
    async def resolve_from_principal(self, principal_id: UUID, **kwargs: Any) -> Any:
        _ = principal_id, kwargs


@attrs.define(eq=False)
class _Stack:
    ctx: ExecutionContext
    principal_reads: list[str]
    epochs: MockPrincipalEpochStore
    spec: AuthnSpec = SPEC
    authz_route: str = ROUTE

    async def principal(self) -> UUID:
        created = await self.ctx.doc.command(policy_principal_spec).create(
            CreatePolicyPrincipalCmd(kind="user"),
        )
        return created.id

    async def run(self, work: Callable[[], Awaitable[T]]) -> T:
        return await work()

    async def in_tx(self, work: Callable[[], Awaitable[T]]) -> T:
        async with self.ctx.tx_ctx.scope(TX):
            return await work()

    async def epoch(self, principal: UUID) -> int:
        return await self.epochs.current(principal)


def _stack(
    *,
    authz: RevocationStates | None = CONFIG,
    authn: RevocationStates | None = CONFIG,
) -> _Stack:
    mock = MockDepsModule()
    state = mock.state
    reads: list[str] = []
    document = ConfigurableMockDocument(module=mock)
    epochs = MockPrincipalEpochStore(state=state, in_transaction=lambda: False)
    states = MockRevocationStates(state=state, namespace=STATES)

    def principals(ctx: ExecutionContext, spec: Any) -> Any:
        return CountingPrincipalReads(document(ctx, spec), reads=reads)

    def epoch_store(ctx: ExecutionContext) -> MockPrincipalEpochStore:
        return MockPrincipalEpochStore(state=state, in_transaction=lambda: ctx.tx_ctx.depth() > 0)

    kernel = AuthnKernelConfig(
        access_token_secret=b"k" * 32,
        refresh_token_pepper=b"p" * 32,
        password=PasswordConfig(time_cost=1, memory_cost=8, parallelism=1),
        revocation_states=authn,
    )
    deps = (
        AuthnDepsModule(
            kernel=kernel,
            authn={ROUTE: SPEC.enabled_methods},
            authz_route=ROUTE,
            token_lifecycle={ROUTE},
            password_account_provisioning={ROUTE},
            principal_deactivation={ROUTE},
        )()
        .merge(
            AuthzDepsModule(
                kernel=AuthzKernelConfig(revocation_states=authz),
                principal_registry={ROUTE},
            )()
        )
        .merge(
            Deps.routed(
                {
                    DocumentQueryDepKey: {
                        session_spec.name: document,
                        policy_principal_spec.name: principals,
                        password_account_spec.name: document,
                        api_key_account_spec.name: document,
                    },
                    DocumentCommandDepKey: dict.fromkeys(
                        (
                            session_spec.name,
                            policy_principal_spec.name,
                            password_account_spec.name,
                            api_key_account_spec.name,
                        ),
                        document,
                    ),
                    RevocationStatesDepKey: {STATES: lambda ctx, spec: states},
                    TenantResolverDepKey: {ROUTE: lambda ctx: _AnyTenant()},
                }
            )
        )
        .merge(
            Deps.plain(
                {
                    MockStateDepKey: state,
                    TransactionManagerDepKey: mock_journal_txmanager,
                    PrincipalEpochDepKey: epoch_store,
                }
            )
        )
    )

    return _Stack(ctx=context_from_deps(deps), principal_reads=reads, epochs=epochs)


# ----------------------- #


async def test_the_mock_never_skips_the_read() -> None:
    # Its locking read only claims the row, so a deactivation does not exclude an issue.
    stack = _stack()
    issued = await battery.login(stack, await stack.principal())

    for _ in range(2):
        stack.principal_reads.clear()
        assert await battery.verify(stack, issued) == "admitted"
        assert stack.principal_reads == ["find"]


@pytest.mark.parametrize("path", sorted(battery.DEACTIVATIONS))
async def test_a_deactivation_refuses_the_next_verification(path: str) -> None:
    await battery.a_deactivation_refuses_the_next_verification(_stack(), path)


@pytest.mark.parametrize("path", sorted(battery.DEACTIVATIONS))
async def test_a_deactivation_outside_a_transaction_writes_nothing(path: str) -> None:
    await battery.a_deactivation_outside_a_transaction_writes_nothing(_stack(), path)


async def test_an_activation_raises_nothing() -> None:
    await battery.an_activation_raises_nothing(_stack())


# ....................... #


async def test_the_hook_without_the_fast_path_still_raises_the_epoch() -> None:
    stack = _stack(authn=None)
    principal = await stack.principal()

    await stack.in_tx(lambda: battery.DEACTIVATIONS["update"](stack, principal))

    assert await stack.epoch(principal) == 1


async def test_authn_and_authz_with_different_configurations_are_refused() -> None:
    stack = _stack(authz=attrs.evolve(CONFIG, ttl=timedelta(minutes=1)))
    principal = await stack.principal()

    # Refused wherever the eligibility gate is built: here, for the login.
    with pytest.raises(CoreException) as caught:
        await battery.login(stack, principal)

    assert caught.value.code == "authn_revocation_states_mismatch"


async def test_an_inactive_principal_is_created_inactive_outside_a_transaction() -> None:
    stack = _stack()
    registry = stack.ctx.authz.principal_registry(AuthzSpec(name=ROUTE))
    principal = uuid4()

    created = await registry.ensure_principal(principal, "user", is_active=False)

    assert not created.is_active
    assert (await stack.ctx.doc.query(policy_principal_spec).get(principal)).is_active is False


async def test_a_deactivation_outside_a_transaction_writes_nothing_first() -> None:
    # Authz raises epochs while authn has no fast path, so nothing makes authn refuse up
    # front: the principal's write goes first, and its refusal leaves the credentials open.
    stack = _stack(authn=None)
    principal = await stack.principal()
    credentials = PasswordCredentials(login="user@example.com", password="horse battery")
    await stack.ctx.authn.password_account_provisioning(SPEC).register_with_password(
        principal, credentials
    )

    with pytest.raises(CoreException) as caught:
        await stack.ctx.authn.principal_deactivation(SPEC).deactivate(principal)

    assert caught.value.code == "authn_revocation_requires_transaction"
    assert (await stack.ctx.authn.authn(SPEC).authenticate_with_password(credentials)).identity


class _ElsewherePort:
    """A command port whose writes run outside the open transaction (another database)."""

    def __init__(self) -> None:
        self.writes: list[str] = []

    def is_transactionally_enlisted(self) -> bool:
        return False

    async def update(self, pk: UUID, rev: int, dto: Any, **kwargs: Any) -> None:
        self.writes.append("update")


async def test_a_principal_write_the_transaction_does_not_hold_is_refused_before_it_runs() -> None:
    # The epochs are in the open transaction; the principal's own write is not, so it would
    # commit whether or not the raise that follows it does.
    stack = _stack()
    hook = PolicyPrincipalEpochHook(config=CONFIG)
    port = _ElsewherePort()
    guarded = hook(stack.ctx, port)  # type: ignore[arg-type]

    with pytest.raises(CoreException) as caught:
        await stack.in_tx(
            lambda: guarded.update(uuid4(), 1, UpdatePolicyPrincipalCmd(is_active=False))
        )

    assert caught.value.code == "authn_revocation_requires_transaction"
    assert port.writes == []


class _AsyncPort:
    """A policy principal command port that records the patches reaching it."""

    def __init__(self) -> None:
        self.writes: list[Any] = []

    async def update(self, pk: UUID, rev: int, dto: Any, **kwargs: Any) -> None:
        self.writes.append(dto)


class _SyncPort(_AsyncPort):
    """The same, its write a plain method returning an awaitable."""

    def update(self, pk: UUID, rev: int, dto: Any, **kwargs: Any) -> Any:  # type: ignore[override]
        self.writes.append(dto)

        async def done() -> None:
            return None

        return done()


@pytest.mark.parametrize(
    ("port", "patch"),
    [
        pytest.param(_AsyncPort, {"is_active": False}, id="mapping"),
        pytest.param(_AsyncPort, object(), id="unknown-shape"),
        pytest.param(_SyncPort, UpdatePolicyPrincipalCmd(is_active=False), id="sync-method"),
    ],
)
async def test_a_deactivating_patch_of_any_shape_raises_the_epoch(port: type, patch: Any) -> None:
    # A mapping, a patch of no known shape (assumed to deactivate) and a write method that is
    # not a coroutine function all reach the hook: refused outside a transaction before the
    # write, raised inside one.
    stack = _stack()
    inner = port()
    guarded = PolicyPrincipalEpochHook(config=CONFIG)(stack.ctx, inner)
    principal = uuid4()

    with pytest.raises(CoreException) as caught:
        await guarded.update(principal, 1, patch)

    assert caught.value.code == "authn_revocation_requires_transaction"
    assert inner.writes == []

    await stack.in_tx(lambda: guarded.update(principal, 1, patch))

    assert inner.writes == [patch]
    assert await stack.epoch(principal) == 1


async def test_an_activating_mapping_patch_passes_without_a_transaction() -> None:
    stack = _stack()
    inner = _AsyncPort()
    guarded = PolicyPrincipalEpochHook(config=CONFIG)(stack.ctx, inner)
    principal = uuid4()

    await guarded.update(principal, 1, {"is_active": True})

    assert inner.writes == [{"is_active": True}]
    assert await stack.epoch(principal) == 0
