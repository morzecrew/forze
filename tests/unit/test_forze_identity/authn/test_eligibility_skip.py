"""Skipping the eligibility read after a fast-path admission, on the identity plane's adapters.

The adapters run over mock documents, the mock's journal transactions and the mock states and
epoch stores. The shared battery is the same one Postgres and Redis run; the cases here are the
configuration and credential cases that need the mock's flexibility.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, Literal, TypeVar
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.contracts.authn import (
    EPOCH_STATE_KEY,
    ApiKeyCredentials,
    AuthnIdentity,
    AuthnSpec,
    PasswordCredentials,
    PrincipalEligibilityDepKey,
    PrincipalEpochDepKey,
    RevocationStatesDepKey,
)
from forze.application.contracts.authz import AuthzSpec
from forze.application.contracts.deps import backend_of
from forze.application.contracts.deps.proxy import PortProxy
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.tenancy import TenantResolverDepKey
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze_identity.authn import (
    AuthnDepsModule,
    AuthnKernelConfig,
    RevocationStates,
    StateMacKeys,
)
from forze_identity.authn.application.specs import (
    api_key_account_spec,
    password_account_spec,
    session_spec,
)
from forze_identity.authn.execution.deps import deps as authn_deps
from forze_identity.authn.services import (
    ApiKeyRevocation,
    EpochRaise,
    PasswordConfig,
    PolicyPrincipalEpochHook,
    revocation_states,
)
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
from tests.support.eligibility_skip_conformance import (
    CountingEpochReads,
    CountingPrincipalReads,
)
from tests.support.execution_context import context_from_deps

pytest.importorskip("argon2")

pytestmark = pytest.mark.unit

T = TypeVar("T")

# ----------------------- #

ROUTE = "main"
SPEC = AuthnSpec(name=ROUTE, enabled_methods=frozenset({"token", "password", "api_key"}))
STATES = "authn-revocations"
TX = "default"
CONFIG = RevocationStates(
    route=STATES,
    ttl=timedelta(minutes=5),
    key_mac=StateMacKeys(active=b"m" * 32),
)


class _AnyTenant:
    async def resolve_from_principal(self, principal_id: UUID, **kwargs: Any) -> Any:
        _ = principal_id, kwargs


@attrs.define(eq=False)
class _Stack:
    ctx: ExecutionContext
    principal_reads: list[str]
    epochs: MockPrincipalEpochStore
    key_reads: list[str] = attrs.field(factory=list)
    epoch_reads: list[str] = attrs.field(factory=list)
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
    eligibility: Literal["policy_principal", "allow_all"] = "policy_principal",
    actor_claim: str | None = None,
) -> _Stack:
    mock = MockDepsModule()
    state = mock.state
    reads: list[str] = []
    document = ConfigurableMockDocument(module=mock)
    epochs = MockPrincipalEpochStore(state=state, in_transaction=lambda: False)
    states = MockRevocationStates(state=state, namespace=STATES)

    key_reads: list[str] = []

    def principals(ctx: ExecutionContext, spec: Any) -> Any:
        return CountingPrincipalReads(document(ctx, spec), reads=reads)

    def keys(ctx: ExecutionContext, spec: Any) -> Any:
        return CountingPrincipalReads(document(ctx, spec), reads=key_reads)

    epoch_reads: list[str] = []

    def epoch_store(ctx: ExecutionContext) -> Any:
        return CountingEpochReads(
            MockPrincipalEpochStore(state=state, in_transaction=lambda: ctx.tx_ctx.depth() > 0),
            reads=epoch_reads,
        )

    kernel = AuthnKernelConfig(
        access_token_secret=b"k" * 32,
        refresh_token_pepper=b"p" * 32,
        api_key_pepper=b"a" * 32,
        password=PasswordConfig(time_cost=1, memory_cost=8, parallelism=1),
        revocation_states=authn,
    )
    deps = (
        AuthnDepsModule(
            kernel=kernel,
            eligibility=eligibility,
            actor_claim=actor_claim,
            authn={ROUTE: SPEC.enabled_methods},
            authz_route=ROUTE,
            token_lifecycle={ROUTE},
            api_key_lifecycle={ROUTE},
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
                        api_key_account_spec.name: keys,
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

    return _Stack(
        ctx=context_from_deps(deps),
        principal_reads=reads,
        epochs=epochs,
        key_reads=key_reads,
        epoch_reads=epoch_reads,
    )


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


def test_the_skip_is_refused_without_signed_epochs() -> None:
    unsigned = attrs.evolve(CONFIG, key_mac=None)

    with pytest.raises(CoreException) as caught:
        AuthzKernelConfig(revocation_states=unsigned)

    assert caught.value.code == "authn_eligibility_skip_requires_key_mac"


class _Principals:
    """A policy principal query port recording its locking reads into *calls*."""

    def __init__(self, calls: list[tuple[str, Any]], *, found: bool, enlisted: bool) -> None:
        self.calls, self._found, self._enlisted = calls, found, enlisted

    def is_transactionally_enlisted(self) -> bool:
        return self._enlisted

    async def find(self, *, filters: Any, for_update: bool = False) -> Any:
        self.calls.append(("find", for_update))
        return object() if self._found else None


class _Epochs:
    """An epoch store recording its raises into *calls*."""

    def __init__(self, calls: list[tuple[str, Any]]) -> None:
        self.calls = calls

    async def advance(self, principal_id: UUID) -> int:
        self.calls.append(("advance", principal_id))
        return 1


async def _never_runs(callback: Any) -> None:
    del callback


@pytest.mark.parametrize(
    ("found", "enlisted", "calls"),
    [
        (True, True, [("find", True), "advance"]),
        (False, True, [("find", True), "advance"]),
        (True, False, ["advance"]),
    ],
)
async def test_an_epoch_raise_locks_the_principal_before_the_epoch(
    found: bool, enlisted: bool, calls: list[Any]
) -> None:
    # A principal with no row (deleted) is raised all the same: the lock read finds nothing.
    # Principals the open transaction does not hold (another database) are not locked.
    recorded: list[tuple[str, Any]] = []
    principal = uuid4()
    raise_epoch = EpochRaise(
        config=CONFIG,
        states=MockRevocationStates(state=MockDepsModule().state, namespace=STATES),
        epochs=_Epochs(recorded),  # type: ignore[arg-type]
        in_transaction=lambda: True,
        defer=_never_runs,
        principals=_Principals(recorded, found=found, enlisted=enlisted),  # type: ignore[arg-type]
    )

    await raise_epoch(principal)

    assert recorded == [("advance", principal) if call == "advance" else call for call in calls]


async def test_a_logout_locks_no_principal_on_the_mock() -> None:
    # The mock's locking read only claims the row: the raise keeps to the epoch, as the skip does.
    stack = _stack()
    principal = await stack.principal()
    stack.principal_reads.clear()

    await stack.in_tx(
        lambda: stack.ctx.authn.token_lifecycle(SPEC).revoke_tokens(
            AuthnIdentity(principal_id=principal)
        )
    )

    assert stack.principal_reads == []
    assert await stack.epoch(principal) == 1


@pytest.mark.parametrize(
    ("eligibility", "reads"),
    [("policy_principal", ["find"]), ("allow_all", [])],
)
async def test_a_logout_locks_the_principal_only_where_an_issue_does(
    eligibility: Literal["policy_principal", "allow_all"],
    reads: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # As on Postgres. Under allow_all an issue locks no principal, so a raise has no order to match.
    monkeypatch.setattr(authn_deps, "_locks_exclude", lambda principals: True)
    monkeypatch.setattr(revocation_states, "enlisted", lambda port: True)
    stack = _stack(eligibility=eligibility)
    principal = await stack.principal()
    stack.principal_reads.clear()

    await stack.in_tx(
        lambda: stack.ctx.authn.token_lifecycle(SPEC).revoke_tokens(
            AuthnIdentity(principal_id=principal)
        )
    )

    assert stack.principal_reads == reads


@pytest.mark.parametrize(
    ("eligibility_at", "principals", "locks"),
    [("routed", True, True), ("plain", True, True), (None, True, False), ("routed", False, False)],
)
def test_a_raise_finds_the_eligibility_an_issue_falls_back_to(
    eligibility_at: Literal["routed", "plain"] | None,
    principals: bool,
    locks: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An issue resolves eligibility through provide, which falls back to a plain registration.
    monkeypatch.setattr(authn_deps, "_locks_exclude", lambda principals: True)

    def eligibility(ctx: ExecutionContext, spec: Any) -> Any:
        raise AssertionError("only looked up, never built")

    deps = Deps.routed(
        {DocumentQueryDepKey: {policy_principal_spec.name: lambda ctx, spec: object()}}
        if principals
        else {}
    )

    if eligibility_at == "routed":
        deps = deps.merge(Deps.routed({PrincipalEligibilityDepKey: {ROUTE: eligibility}}))

    elif eligibility_at == "plain":
        deps = deps.merge(Deps.plain({PrincipalEligibilityDepKey: eligibility}))

    found = authn_deps._principals_to_lock(context_from_deps(deps), SPEC)

    assert (found is not None) is locks


# ....................... #
# API keys. The mock refuses the skip, so these stand it on Postgres's footing.


def _as_postgres(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(authn_deps, "_on_skip_backend", lambda port: True)


def _key_states(stack: _Stack) -> dict[Any, tuple[str, int]]:
    """The mock store's API-key states, by their stored key."""

    states = stack.ctx.deps.provide(MockStateDepKey).revocation_states
    return {stored: entry for stored, entry in states.items() if stored[2].startswith("k:")}


async def test_a_warm_api_key_reads_neither_its_row_nor_the_principal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _as_postgres(monkeypatch)
    await battery.a_warm_api_key_reads_neither_its_row_nor_the_principal(_stack())


@pytest.mark.parametrize("path", sorted(battery.DEACTIVATIONS))
async def test_a_deactivation_refuses_a_warm_api_key(
    path: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _as_postgres(monkeypatch)
    await battery.a_deactivation_refuses_a_warm_api_key(_stack(), path)


@pytest.mark.parametrize("postgres", [True, False], ids=["no-authz-configuration", "mock"])
async def test_without_the_skip_every_api_key_verification_reads(
    postgres: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if postgres:
        _as_postgres(monkeypatch)

    stack = _stack(authz=None) if postgres else _stack()
    key = await battery.issue_key(stack, await stack.principal())

    for _ in range(3):
        stack.principal_reads.clear()
        assert await battery.verify_key(stack, key) == "admitted"
        assert stack.principal_reads == ["find"]

    # Without the skip the state records no epoch, and reads as an older version's.
    ((state, _),) = _key_states(stack).values()
    assert state.count("|") == 5


async def test_a_state_without_an_epoch_reads_eligibility(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # As one an older version wrote: admitted from the state, then read.
    _as_postgres(monkeypatch)
    stack = _stack()
    key = await battery.issue_key(stack, await stack.principal())

    async def none(self: Any, principal_id: UUID) -> None:
        del self, principal_id

    with monkeypatch.context() as patched:
        patched.setattr(ApiKeyRevocation, "_eligible_epoch", none)
        assert await battery.verify_key(stack, key) == "admitted"

    stack.principal_reads.clear()
    stack.key_reads.clear()
    assert await battery.verify_key(stack, key) == "admitted"
    assert (stack.key_reads, stack.principal_reads) == ([], ["find"])


async def test_an_inactive_principal_records_no_epoch(monkeypatch: pytest.MonkeyPatch) -> None:
    # Read after the epoch, so a deactivation before that read leaves the state unable to skip.
    _as_postgres(monkeypatch)
    stack = _stack()
    principal = await stack.principal()
    key = await battery.issue_key(stack, principal)
    command = stack.ctx.doc.command(policy_principal_spec)
    rev = (await stack.ctx.doc.query(policy_principal_spec).get(principal)).rev

    async def deactivate() -> None:
        await command.update(principal, rev, UpdatePolicyPrincipalCmd(is_active=False))

    await stack.in_tx(deactivate)

    # The state the first verification writes must not let the next one skip.
    for _ in range(2):
        assert await battery.verify_key(stack, key) != "admitted"

    ((state, _),) = _key_states(stack).values()
    assert state.count("|") == 5


@pytest.mark.parametrize("planted", ["raised", "another_principals"])
async def test_an_altered_epoch_fails_the_states_mac(
    planted: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _as_postgres(monkeypatch)
    stack = _stack()
    principal, other = await stack.principal(), await stack.principal()
    key = await battery.issue_key(stack, principal)
    assert await battery.verify_key(stack, key) == "admitted"

    # The other principal's epoch, raised by a logout everywhere.
    await stack.in_tx(
        lambda: stack.ctx.authn.token_lifecycle(SPEC).revoke_tokens(
            AuthnIdentity(principal_id=other)
        )
    )
    states = stack.ctx.deps.provide(MockStateDepKey).revocation_states
    ((stored, (state, expire_at_ms)),) = _key_states(stack).items()
    live, owner, actor, expires, epoch, not_after, mac = state.split("|")
    altered = str(int(epoch) + 1) if planted == "raised" else str(await stack.epoch(other))
    assert altered != epoch
    states[stored] = (
        "|".join((live, owner, actor, expires, altered, not_after, mac)),
        expire_at_ms,
    )

    for _ in range(2):
        stack.key_reads.clear()
        stack.principal_reads.clear()
        assert await battery.verify_key(stack, key) == "admitted"
        # A miss, read from the row. The planted state stays, so nothing is written over it.
        assert (stack.key_reads, stack.principal_reads) == (["find"], ["find"])


async def _warm_key(stack: _Stack, principal: UUID) -> ApiKeyCredentials:
    key = await battery.issue_key(stack, principal)

    for _ in range(2):
        assert await battery.verify_key(stack, key) == "admitted"

    return key


def _drop_cached_epoch(stack: _Stack, principal: UUID) -> None:
    states = stack.ctx.deps.provide(MockStateDepKey).revocation_states
    del states[(STATES, str(principal), EPOCH_STATE_KEY)]


async def test_a_deactivation_refuses_a_key_whose_cached_epoch_is_gone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Then the database's epoch is compared: the raised one.
    _as_postgres(monkeypatch)
    stack = _stack()
    principal = await stack.principal()
    key = await _warm_key(stack, principal)

    await stack.in_tx(lambda: battery.DEACTIVATIONS["registry"](stack, principal))
    _drop_cached_epoch(stack, principal)

    assert await battery.verify_key(stack, key) != "admitted"


@pytest.mark.parametrize("cached", [True, False], ids=["cached epoch", "no cached epoch"])
async def test_inside_a_transaction_a_key_reads_eligibility(
    cached: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The transaction may have raised the epoch itself, which reaches the cache after commit.
    _as_postgres(monkeypatch)
    stack = _stack()
    principal = await stack.principal()
    key = await _warm_key(stack, principal)

    if not cached:
        _drop_cached_epoch(stack, principal)

    async def request() -> None:
        await stack.ctx.authn.authn(SPEC).authenticate_with_api_key(key)

    stack.principal_reads.clear()
    await asyncio.create_task(stack.in_tx(request))

    assert stack.principal_reads == ["find"]


async def test_a_failed_epoch_read_reads_eligibility_and_counts_a_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _as_postgres(monkeypatch)
    stack = _stack()
    key = await _warm_key(stack, await stack.principal())
    read = MockRevocationStates.read

    async def failing(self: MockRevocationStates, principal_id: UUID, key: str) -> Any:
        if key == EPOCH_STATE_KEY:
            raise RuntimeError("states store down")

        return await read(self, principal_id, key)

    monkeypatch.setattr(MockRevocationStates, "read", failing)
    before = CONFIG.counters.get("fallback", "api_key")
    stack.principal_reads.clear()

    assert await battery.verify_key(stack, key) == "admitted"
    assert stack.principal_reads == ["find"]
    assert CONFIG.counters.get("fallback", "api_key") == before + 1


async def test_a_failed_database_epoch_read_reads_eligibility_and_counts_a_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _as_postgres(monkeypatch)
    current = MockPrincipalEpochStore.current
    down = False

    # Patched before the stack is built: its counting wrapper binds the method then.
    async def failing(self: MockPrincipalEpochStore, principal_id: UUID) -> int:
        if down:
            raise RuntimeError("epoch store down")

        return await current(self, principal_id)

    monkeypatch.setattr(MockPrincipalEpochStore, "current", failing)
    stack = _stack()
    principal = await stack.principal()
    key = await _warm_key(stack, principal)
    _drop_cached_epoch(stack, principal)
    down = True
    before = CONFIG.counters.get("fallback", "api_key")
    stack.principal_reads.clear()

    assert await battery.verify_key(stack, key) == "admitted"
    assert stack.principal_reads == ["find"]
    assert CONFIG.counters.get("fallback", "api_key") == before + 1


@pytest.mark.parametrize("registration", ["routed", "plain"])
def test_a_key_skips_with_eligibility_registered_plain(
    registration: Literal["routed", "plain"],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An issue resolves eligibility through provide, which falls back to a plain registration.
    monkeypatch.setattr(authn_deps, "_skips_eligibility", lambda *args: True)
    eligibility = authn_deps.ConfigurablePolicyPrincipalEligibility()
    deps = Deps.routed(
        {DocumentQueryDepKey: {policy_principal_spec.name: lambda ctx, spec: object()}}
    ).merge(
        Deps.plain({PrincipalEpochDepKey: lambda ctx: object()}),
        Deps.routed({PrincipalEligibilityDepKey: {ROUTE: eligibility}})
        if registration == "routed"
        else Deps.plain({PrincipalEligibilityDepKey: eligibility}),
    )

    skip = authn_deps._api_key_skip(context_from_deps(deps), SPEC, CONFIG, object())  # type: ignore[arg-type]

    assert skip is not None


@attrs.define(slots=True)
class _Pause:
    """Holds the first call that reaches it until released; every later one passes."""

    reached: asyncio.Event = attrs.field(factory=asyncio.Event)
    release: asyncio.Event = attrs.field(factory=asyncio.Event)

    async def once(self) -> None:
        if self.reached.is_set():
            return

        self.reached.set()
        await asyncio.wait_for(self.release.wait(), timeout=10)


@attrs.define(slots=True)
class _PausedAfter(PortProxy):
    """A port whose *names* calls each stop at *pause* once they return."""

    names: frozenset[str] = attrs.field(kw_only=True)
    pause: _Pause = attrs.field(kw_only=True)

    def _should_wrap(self, name: str, attr: Any) -> bool:
        del attr
        return name in self.names

    def _wrap_async(self, name: str, attr: Any) -> Any:
        async def paused(*args: Any, **kwargs: Any) -> Any:
            result = await attr(*args, **kwargs)
            await self.pause.once()
            return result

        return paused


async def test_a_deactivation_between_the_misss_two_reads_leaves_no_epoch_to_skip_by(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Whichever of the epoch and the principal the miss reads first, a deactivation then lands
    # before the other. Epoch first, the principal reads inactive and no epoch is recorded.
    _as_postgres(monkeypatch)
    pause = _Pause()
    skip_for = authn_deps._api_key_skip

    def paused_skip(*args: Any) -> Any:
        skip = skip_for(*args)

        if skip is None:
            return None

        return attrs.evolve(
            skip,
            epochs=_PausedAfter(skip.epochs, names=frozenset({"current"}), pause=pause),
            principals=_PausedAfter(skip.principals, names=frozenset({"find"}), pause=pause),
        )

    monkeypatch.setattr(authn_deps, "_api_key_skip", paused_skip)
    stack = _stack()
    principal = await stack.principal()
    key = await battery.issue_key(stack, principal)

    async with asyncio.timeout(10):
        verifying = asyncio.create_task(battery.verify_key(stack, key))
        await pause.reached.wait()
        await stack.in_tx(lambda: battery.DEACTIVATIONS["update"](stack, principal))
        pause.release.set()
        assert await verifying != "admitted"

    assert await battery.verify_key(stack, key) != "admitted"


async def test_the_skip_needs_the_epochs_on_postgres(monkeypatch: pytest.MonkeyPatch) -> None:
    # Another epoch store need not report its enlistment, so its raise may commit on its own.
    monkeypatch.setattr(
        authn_deps,
        "_on_skip_backend",
        lambda port: not isinstance(backend_of(port), MockPrincipalEpochStore),
    )
    stack = _stack()
    principal = await stack.principal()
    issued = await battery.login(stack, principal)
    key = await battery.issue_key(stack, principal)

    for _ in range(2):
        stack.principal_reads.clear()
        assert await battery.verify(stack, issued) == "admitted"
        assert stack.principal_reads == ["find"]

        stack.principal_reads.clear()
        assert await battery.verify_key(stack, key) == "admitted"
        assert stack.principal_reads == ["find"]


@pytest.mark.parametrize("self_acting", [False, True], ids=["agent", "subject"])
async def test_a_delegated_keys_actor_is_always_read(
    self_acting: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _as_postgres(monkeypatch)
    stack = _stack(actor_claim="act")
    principal = await stack.principal()
    keys = stack.ctx.authn.api_key_lifecycle(SPEC)

    if self_acting:
        # Refused at issue; a row written around it names the subject all the same.
        issued = await stack.in_tx(
            lambda: keys._issue_for_principal(principal, actor_principal_id=principal)
        )

    else:
        agent = await stack.ctx.doc.command(policy_principal_spec).create(
            CreatePolicyPrincipalCmd(kind="service"),
        )
        issued = await stack.in_tx(
            lambda: keys.issue_api_key(
                AuthnIdentity(principal_id=principal), actor_principal_id=agent.id
            )
        )

    assert await battery.verify_key(stack, issued.key) == "admitted"

    for _ in range(2):
        stack.principal_reads.clear()
        assert await battery.verify_key(stack, issued.key) == "admitted"
        # The subject's check skipped; the actor's read, even when it is the subject.
        assert stack.principal_reads == ["find"]
