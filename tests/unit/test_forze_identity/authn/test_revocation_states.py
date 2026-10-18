"""Verifying sessions from cached revocation states, on the identity plane's real adapters.

The adapters run over mock documents, the mock's journal transactions, and the mock states and
epoch stores. A states proxy counts and gates calls, and can fail them; a counting proxy over
the session store and the epoch store shows which verifications read the database.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import attrs
import jwt
import pytest
import structlog

from forze.application.contracts.authn import (
    ApiKeyStateRead,
    AuthnIdentity,
    AuthnSpec,
    IssuedTokens,
    PasswordCredentials,
    PrincipalEpochDepKey,
    RefreshTokenCredentials,
    RevocationStatesDepKey,
    RevocationStatesRead,
)
from forze.application.contracts.authz import PrincipalRegistryDepKey
from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.contracts.tenancy import TenantResolverDepKey
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException, exc
from forze_identity.authn import (
    AuthnDepsModule,
    AuthnKernelConfig,
    ForzeJwtTokenVerifier,
    RevocationStates,
)
from forze_identity.authn.application.specs import (
    api_key_account_spec,
    password_account_spec,
    password_invite_spec,
    password_reset_spec,
    session_spec,
)
from forze_identity.authn.services import (
    AccessTokenService,
    ApiKeyConfig,
    Hs256Signer,
    PasswordConfig,
    SessionBinding,
    StateMacKeys,
)
from forze_identity.authz.application.specs import policy_principal_spec
from forze_identity.authz.domain.models.policy_principal import CreatePolicyPrincipalCmd
from forze_identity.authz.execution.deps.deps import ConfigurablePrincipalRegistry
from forze_kits.aggregates.authn import (
    AuthnIssueApiKeyRequestDTO,
    AuthnKernelOp,
    AuthnLoginRequestDTO,
    AuthnRefreshRequestDTO,
    build_authn_registry,
)
from forze_kits.aggregates.tenancy import TenancyKernelOp, build_tenancy_registry
from forze_kits.aggregates.tenancy.dto import TenantSwitchRequestDTO
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.adapters.revocation import MockPrincipalEpochStore, MockRevocationStates
from forze_mock.execution.factories import ConfigurableMockDocument, mock_journal_txmanager
from tests.support.execution_context import context_from_deps

pytest.importorskip("argon2")

pytestmark = pytest.mark.unit

# ----------------------- #

ROUTE = "main"
SPEC = AuthnSpec(name=ROUTE, enabled_methods=frozenset({"token", "password", "api_key"}))
NS = SPEC.default_namespace
STATES = "authn-revocations"
TTL = timedelta(minutes=5)
TX = "default"


@attrs.define
class _Clock:
    now: float = 1_000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, delta: timedelta) -> None:
        self.now += delta.total_seconds()


@attrs.define(eq=False)
class _Gate:
    """Holds one call open until released."""

    reached: asyncio.Event = attrs.field(factory=asyncio.Event)
    release: asyncio.Event = attrs.field(factory=asyncio.Event)

    async def hold(self) -> None:
        self.reached.set()
        await asyncio.wait_for(self.release.wait(), timeout=5)


@attrs.define(eq=False)
class _States:
    """The mock states store, with calls that can be counted, held, or failed."""

    inner: MockRevocationStates
    down: bool = False
    writes_down: bool = False
    fill_down: bool = False
    fill_gate: _Gate | None = None
    calls: list[str] = attrs.field(factory=list)

    def _call(self, name: str, *, write: bool) -> None:
        self.calls.append(name)

        if self.down or (write and self.writes_down):
            raise exc.infrastructure("states store unavailable")

    async def read(self, principal_id: UUID, key: str) -> RevocationStatesRead:
        self._call("read", write=False)
        return await self.inner.read(principal_id, key)

    async def fill(
        self, principal_id: UUID, states: Mapping[str, str], *, expire_at_ms: int
    ) -> None:
        self._call("fill", write=False)

        if self.fill_down:
            raise exc.infrastructure("states store unavailable")

        if self.fill_gate is not None:
            gate, self.fill_gate = self.fill_gate, None
            await gate.hold()

        await self.inner.fill(principal_id, states, expire_at_ms=expire_at_ms)

    async def revoke(self, principal_id: UUID, keys: Sequence[str], *, ttl: timedelta) -> None:
        self._call("revoke", write=True)
        await self.inner.revoke(principal_id, keys, ttl=ttl)

    async def now_ms(self, principal_id: UUID) -> int:
        self._call("now", write=True)
        return await self.inner.now_ms(principal_id)

    async def raise_epoch(self, principal_id: UUID, epoch: int, *, expire_at_ms: int) -> None:
        self._call("raise_epoch", write=True)
        await self.inner.raise_epoch(principal_id, epoch, expire_at_ms=expire_at_ms)

    async def read_api_key(self, digest: str) -> ApiKeyStateRead:
        self._call("read_api_key", write=False)
        return await self.inner.read_api_key(digest)

    async def fill_api_key(self, digest: str, state: str, *, expire_at_ms: int) -> None:
        self._call("fill_api_key", write=False)

        if self.fill_down:
            raise exc.infrastructure("states store unavailable")

        await self.inner.fill_api_key(digest, state, expire_at_ms=expire_at_ms)

    async def revoke_api_keys(self, digests: Sequence[str], *, ttl: timedelta) -> None:
        self._call("revoke_api_keys", write=True)
        await self.inner.revoke_api_keys(digests, ttl=ttl)


@attrs.define(eq=False)
class _Epochs:
    """The mock epoch store, counting reads and able to hold the next one before or after it."""

    inner: MockPrincipalEpochStore
    reads: list[str]
    before_read: _Gate | None = None
    after_read: _Gate | None = None

    async def current(self, principal_id: UUID) -> int:
        self.reads.append("epoch")

        if self.before_read is not None:
            gate, self.before_read = self.before_read, None
            await gate.hold()

        epoch = await self.inner.current(principal_id)

        if self.after_read is not None:
            gate, self.after_read = self.after_read, None
            await gate.hold()

        return epoch

    async def hold(self, principal_id: UUID) -> int:
        return await self.inner.hold(principal_id)

    async def advance(self, principal_id: UUID) -> int:
        return await self.inner.advance(principal_id)


class _CountingQuery:
    """A document query port that records its reads, as ``<label>.<method>``.

    With *cap*, a ``find_many`` that names no limit returns at most *cap* rows, as the Postgres,
    Mongo and Firestore gateways' implicit limit does.
    """

    def __init__(
        self,
        inner: Any,
        reads: list[str],
        label: str = "session",
        cap: int | None = None,
    ) -> None:
        self._inner = inner
        self._reads = reads
        self._label = label
        self._cap = cap

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._inner, name)

        if name not in ("find", "find_many", "get"):
            return attr

        async def counted(*args: Any, **kwargs: Any) -> Any:
            self._reads.append(f"{self._label}.{name}")
            pagination = kwargs.get("pagination") or {}

            if name == "find_many" and self._cap is not None and pagination.get("limit") is None:
                kwargs["pagination"] = {**pagination, "limit": self._cap}

            return await attr(*args, **kwargs)

        return counted


class _AnyTenant:
    """A tenant resolver that admits every requested tenant."""

    async def resolve_from_principal(self, principal_id: UUID, **kwargs: Any) -> Any:
        _ = principal_id, kwargs


# ....................... #


@attrs.define(eq=False)
class _Stack:
    ctx: ExecutionContext
    clock: _Clock
    states: _States
    epochs: _Epochs
    reads: list[str]

    @property
    def lifecycle(self) -> Any:
        return self.ctx.authn.token_lifecycle(SPEC)

    async def principal(self) -> UUID:
        created = await self.ctx.doc.command(policy_principal_spec).create(
            CreatePolicyPrincipalCmd(kind="user"),
        )
        return created.id

    async def login(self, principal: UUID, *, tenant: UUID | None = None) -> IssuedTokens:
        async with self.ctx.tx_ctx.scope(TX):
            return await self.lifecycle.issue_tokens(
                AuthnIdentity(principal_id=principal),
                tenant_id=tenant,
            )

    async def refresh(self, issued: IssuedTokens) -> IssuedTokens:
        if issued.refresh is None:
            raise AssertionError("no refresh token")

        async with self.ctx.tx_ctx.scope(TX):
            return await self.lifecycle.refresh_tokens(
                RefreshTokenCredentials(token=issued.refresh.token.token),
            )

    async def logout(self, principal: UUID) -> None:
        async with self.ctx.tx_ctx.scope(TX):
            await self.lifecycle.revoke_tokens(AuthnIdentity(principal_id=principal))

    async def revoke_family(self, principal: UUID, issued: IssuedTokens) -> None:
        async with self.ctx.tx_ctx.scope(TX):
            await self.lifecycle.revoke_chain_of_tokens(principal, _claims(issued)["fam"])

    async def verify(self, issued: IssuedTokens) -> str:
        """``admitted``, or the refusal's code, from a request of its own.

        A request runs in its own task, so what its verification records about the caller
        stays there, as it does behind a transport.
        """

        return await asyncio.create_task(self.verify_here(issued))

    async def verify_here(self, issued: IssuedTokens) -> str:
        """Verify in the current task, as the request that then runs an operation does."""

        try:
            await self.ctx.authn.authn(SPEC).authenticate_with_token(issued.access.token)

        except CoreException as error:
            return str(error.code)

        return "admitted"

    async def verify_without_reads(self, issued: IssuedTokens) -> str:
        self.reads.clear()
        code = await self.verify(issued)
        assert self.reads == [], self.reads

        return code


def _claims(issued: IssuedTokens) -> dict[str, Any]:
    return jwt.decode(issued.access.token.token, options={"verify_signature": False})


def _resign(issued: IssuedTokens, claims: dict[str, Any]) -> IssuedTokens:
    """The same access token with other claims, signed with the stack's key."""

    token = jwt.encode(claims, b"k" * 32, algorithm="HS256")

    return IssuedTokens(
        access=attrs.evolve(issued.access, token=attrs.evolve(issued.access.token, token=token)),
        refresh=None,
    )


def _stack(
    *,
    binding: SessionBinding = "session",
    fast_path: bool = True,
    write_attempts: int = 3,
    key_mac: StateMacKeys | None = None,
    clock: _Clock | None = None,
    api_key: ApiKeyConfig | None = None,
    states_route: str = STATES,
    namespace: str = STATES,
    store_clock: bool = True,
    states_port: Callable[[_States], Any] | None = None,
    key_query_cap: int | None = None,
) -> _Stack:
    mock = MockDepsModule()
    state = mock.state
    clock = clock or _Clock()
    reads: list[str] = []
    # Without *store_clock*, the mock keeps its own clock, as an application wiring it does.
    inner = (
        MockRevocationStates(state=state, namespace=namespace, clock=clock)
        if store_clock
        else MockRevocationStates(state=state, namespace=namespace)
    )
    states = _States(inner=inner)
    document = ConfigurableMockDocument(module=mock)
    epochs_by_ctx: dict[int, _Epochs] = {}

    def counted_sessions(ctx: ExecutionContext, spec: Any) -> Any:
        return _CountingQuery(document(ctx, spec), reads)

    def counted_keys(ctx: ExecutionContext, spec: Any) -> Any:
        return _CountingQuery(document(ctx, spec), reads, label="api_key", cap=key_query_cap)

    def epochs(ctx: ExecutionContext) -> _Epochs:
        return epochs_by_ctx.setdefault(
            id(ctx),
            _Epochs(
                inner=MockPrincipalEpochStore(
                    state=state,
                    in_transaction=lambda: ctx.tx_ctx.depth() > 0,
                ),
                reads=reads,
            ),
        )

    kernel = AuthnKernelConfig(
        access_token_secret=b"k" * 32,
        refresh_token_pepper=b"p" * 32,
        reset_token_pepper=b"r" * 32,
        invite_token_pepper=b"i" * 32,
        api_key_pepper=b"a" * 32,
        api_key=api_key or ApiKeyConfig(),
        password=PasswordConfig(time_cost=1, memory_cost=8, parallelism=1),
        revocation_states=(
            RevocationStates(
                route=states_route,
                ttl=TTL,
                write_attempts=write_attempts,
                session_binding=binding,
                key_mac=key_mac,
            )
            if fast_path
            else None
        ),
    )
    deps = (
        AuthnDepsModule(
            kernel=kernel,
            authn={ROUTE: SPEC.enabled_methods},
            api_key_lifecycle={ROUTE},
            authz_route=ROUTE,
            token_lifecycle={ROUTE},
            password_lifecycle={ROUTE},
            password_reset={ROUTE},
            password_account_provisioning={ROUTE},
            principal_deactivation={ROUTE},
        )()
        .merge(
            Deps.routed(
                {
                    DocumentQueryDepKey: {
                        session_spec.name: counted_sessions,
                        policy_principal_spec.name: document,
                        password_account_spec.name: document,
                        password_reset_spec.name: document,
                        password_invite_spec.name: document,
                        api_key_account_spec.name: counted_keys,
                    },
                    DocumentCommandDepKey: dict.fromkeys(
                        (
                            session_spec.name,
                            policy_principal_spec.name,
                            password_account_spec.name,
                            password_reset_spec.name,
                            password_invite_spec.name,
                            api_key_account_spec.name,
                        ),
                        document,
                    ),
                    PrincipalRegistryDepKey: {ROUTE: ConfigurablePrincipalRegistry()},
                    RevocationStatesDepKey: {
                        states_route: lambda ctx, spec: (
                            states if states_port is None else states_port(states)
                        ),
                    },
                    TenantResolverDepKey: {ROUTE: lambda ctx: _AnyTenant()},
                }
            )
        )
        .merge(
            Deps.plain(
                {
                    MockStateDepKey: state,
                    TransactionManagerDepKey: mock_journal_txmanager,
                    PrincipalEpochDepKey: epochs,
                }
            )
        )
    )
    ctx = context_from_deps(deps)

    return _Stack(ctx=ctx, clock=clock, states=states, epochs=epochs(ctx), reads=reads)


BINDINGS = pytest.mark.parametrize("binding", ["session", "family"])

# ----------------------- #


async def test_tokens_carry_their_family_and_epoch_only_with_the_fast_path() -> None:
    fast, plain = _stack(), _stack(fast_path=False)

    issued = await fast.login(await fast.principal())
    claims = _claims(issued)
    plain_claims = _claims(await plain.login(await plain.principal()))

    assert claims["pep"] == 0
    assert UUID(claims["fam"]) and UUID(claims["sid"])
    assert "fam" not in plain_claims and "pep" not in plain_claims

    # Nothing is written to the states store when a session is issued.
    assert fast.states.calls == []


@BINDINGS
async def test_after_warm_up_verifications_read_neither_sessions_nor_epochs(
    binding: SessionBinding,
) -> None:
    stack = _stack(binding=binding)
    issued = await stack.login(await stack.principal())

    stack.reads.clear()
    assert await stack.verify(issued) == "admitted"
    assert sorted(stack.reads) == [
        "epoch",
        f"session.{'find' if binding == 'session' else 'find_many'}",
    ]

    for _ in range(50):
        assert await stack.verify_without_reads(issued) == "admitted"


async def _revoke_everywhere(stack: _Stack, principal: UUID, flow: str) -> None:
    identity = AuthnIdentity(principal_id=principal)

    if flow == "reset":
        issued = await stack.ctx.authn.password_reset(SPEC).request_reset("alice")
        assert issued is not None

    async with stack.ctx.tx_ctx.scope(TX):
        if flow == "logout":
            await stack.lifecycle.revoke_tokens(identity)

        elif flow == "change":
            await stack.ctx.authn.password_lifecycle(SPEC).change_password(identity, "pw-1", "pw-2")

        elif flow == "reset":
            await stack.ctx.authn.password_reset(SPEC).reset_password(issued.token, "pw-2")

        else:
            await stack.ctx.authn.principal_deactivation(SPEC).deactivate(principal)


@BINDINGS
@pytest.mark.parametrize("flow", ["logout", "change", "reset", "deactivate"])
async def test_a_raised_epoch_refuses_with_no_read_and_a_later_token_is_admitted(
    binding: SessionBinding,
    flow: str,
) -> None:
    stack = _stack(binding=binding)
    principal = await stack.principal()
    await stack.ctx.authn.password_account_provisioning(SPEC).register_with_password(
        principal,
        PasswordCredentials(login="alice", password="pw-1"),
    )
    first, second = await stack.login(principal), await stack.login(principal)

    for issued in (first, second):
        assert await stack.verify(issued) == "admitted"

    await _revoke_everywhere(stack, principal, flow)

    for issued in (first, second):
        assert await stack.verify_without_reads(issued) == "session_revoked"

    if flow == "deactivate":
        return  # an inactive principal is refused new sessions

    later = await stack.login(principal)

    assert _claims(later)["pep"] == 1
    assert await stack.verify(later) == "admitted"


@BINDINGS
async def test_a_family_revocation_refuses_every_session_it_stamped_with_no_read(
    binding: SessionBinding,
) -> None:
    stack = _stack(binding=binding)
    principal = await stack.principal()
    issued = await stack.login(principal)
    other = await stack.login(principal)

    for tokens in (issued, other):
        assert await stack.verify(tokens) == "admitted"

    # Reuse of a rotated refresh token: the kit revokes the family in a transaction of its own.
    refreshed = await stack.refresh(issued)
    assert await stack.verify(refreshed) == "admitted"

    with pytest.raises(CoreException):
        await run_operation(
            build_authn_registry(SPEC).freeze(),
            NS.key(AuthnKernelOp.REFRESH_TOKENS),
            AuthnRefreshRequestDTO(refresh_token=issued.refresh.token.token),  # type: ignore[union-attr]
            stack.ctx,
        )

    assert await stack.verify_without_reads(refreshed) == "session_revoked"
    assert await stack.verify_without_reads(issued) == "session_revoked"

    # Another family of the principal stays live.
    assert await stack.verify_without_reads(other) == "admitted"


async def test_family_binding_admits_a_token_a_refresh_elsewhere_replaced() -> None:
    stack = _stack(binding="family")
    principal = await stack.principal()
    issued = await stack.login(principal)
    assert await stack.verify(issued) == "admitted"

    await stack.refresh(issued)

    assert await stack.verify_without_reads(issued) == "admitted"

    await stack.logout(principal)

    assert await stack.verify_without_reads(issued) == "session_revoked"


async def test_session_binding_refuses_a_token_a_refresh_replaced() -> None:
    stack = _stack(binding="session")
    issued = await stack.login(await stack.principal())
    assert await stack.verify(issued) == "admitted"

    refreshed = await stack.refresh(issued)

    assert await stack.verify_without_reads(issued) == "session_revoked"
    assert await stack.verify(refreshed) == "admitted"


@BINDINGS
async def test_a_flush_costs_one_read_and_admits_nothing_revoked(binding: SessionBinding) -> None:
    stack = _stack(binding=binding)
    principal = await stack.principal()
    revoked, live = await stack.login(principal), await stack.login(principal)

    await stack.revoke_family(principal, revoked)
    for issued in (revoked, live):
        await stack.verify(issued)

    stack.states.inner.state.revocation_states.clear()

    stack.reads.clear()
    assert await stack.verify(revoked) == "session_revoked"
    assert await stack.verify(live) == "admitted"
    assert stack.reads.count("epoch") == 1  # the second verification found the refilled epoch

    for issued, code in ((revoked, "session_revoked"), (live, "admitted")):
        assert await stack.verify_without_reads(issued) == code


@BINDINGS
async def test_with_the_states_store_down_verifications_agree_with_it_up(
    binding: SessionBinding,
) -> None:
    stack = _stack(binding=binding)
    principal, other = await stack.principal(), await stack.principal()
    tenant = uuid4()

    live = await stack.login(principal, tenant=tenant)
    rotated = await stack.login(principal)
    await stack.refresh(rotated)
    family_revoked = await stack.login(principal)
    await stack.revoke_family(principal, family_revoked)
    logged_out = await stack.login(other)
    await stack.logout(other)
    tokens = [live, rotated, family_revoked, logged_out]

    up = [await stack.verify(issued) for issued in tokens]
    stack.states.down = True
    down = [await stack.verify(issued) for issued in tokens]

    assert down == up
    assert up == [
        "admitted",
        "session_revoked" if binding == "session" else "admitted",
        "session_revoked",
        "session_revoked",
    ]

    if binding == "session":
        # What the session row alone answers, with no fast path at all.
        plain = _stack(fast_path=False)
        plain_principal = await plain.principal()
        plain_rotated = await plain.login(plain_principal)
        await plain.refresh(plain_rotated)

        assert await plain.verify(plain_rotated) == up[1]


async def test_a_lookup_never_replaces_a_revocation_it_raced() -> None:
    stack = _stack()
    principal = await stack.principal()
    issued = await stack.login(principal)

    # The lookup read the session as live, then stalls before it writes.
    gate = stack.states.fill_gate = _Gate()
    lookup = asyncio.create_task(stack.verify(issued))
    await asyncio.wait_for(gate.reached.wait(), timeout=5)

    await stack.revoke_family(principal, issued)

    gate.release.set()
    assert await lookup == "admitted"  # it read before the revocation committed

    assert await stack.verify_without_reads(issued) == "session_revoked"


@pytest.mark.parametrize("stall", ["database read", "write"])
async def test_a_lookup_held_past_ttl_after_its_read_writes_nothing_that_lives(stall: str) -> None:
    stack = _stack()
    issued = await stack.login(await stack.principal())

    # Its expiry is fixed when its read ran, however long the database read or the write takes.
    gate = _Gate()

    if stall == "write":
        stack.states.fill_gate = gate

    else:
        stack.epochs.after_read = gate

    lookup = asyncio.create_task(stack.verify(issued))
    await asyncio.wait_for(gate.reached.wait(), timeout=5)

    stack.clock.advance(TTL)
    gate.release.set()
    await lookup

    assert stack.states.inner.state.revocation_states == {}


async def test_a_lookup_inside_a_transaction_writes_nothing() -> None:
    stack = _stack()
    issued = await stack.login(await stack.principal())

    async with stack.ctx.tx_ctx.scope(TX):
        assert await stack.verify(issued) == "admitted"

    assert "fill" not in stack.states.calls
    assert stack.states.inner.state.revocation_states == {}


async def test_a_lookup_racing_an_evicted_revocation_expires_within_ttl_of_the_commit() -> None:
    stack = _stack()
    principal = await stack.principal()
    issued = await stack.login(principal)

    gate = stack.states.fill_gate = _Gate()
    lookup = asyncio.create_task(stack.verify(issued))
    await asyncio.wait_for(gate.reached.wait(), timeout=5)

    await stack.revoke_family(principal, issued)
    committed_at = stack.clock.now
    stack.states.inner.state.revocation_states.clear()  # the revocation's keys are evicted

    gate.release.set()
    await lookup

    # The stale live state admits inside the window, and is gone at its end.
    assert await stack.verify_without_reads(issued) == "admitted"

    stack.clock.now = committed_at + TTL.total_seconds()
    assert await stack.verify(issued) == "session_revoked"


async def test_a_delayed_epoch_write_held_before_its_read_writes_the_newer_epoch() -> None:
    stack = _stack()
    principal = await stack.principal()
    issued = await stack.login(principal)
    assert await stack.verify(issued) == "admitted"

    # The first logout commits, then its write-through stalls before reading the epoch.
    gate = stack.epochs.before_read = _Gate()
    first = asyncio.create_task(stack.logout(principal))
    await asyncio.wait_for(gate.reached.wait(), timeout=5)

    between = await stack.login(principal)
    await stack.logout(principal)
    stack.states.inner.state.revocation_states.clear()  # the newer epoch's key is lost

    gate.release.set()
    await first

    cached = await stack.states.inner.read(principal, "s:-")
    assert cached.epoch == "2"

    for tokens in (issued, between):
        assert await stack.verify_without_reads(tokens) == "session_revoked"


async def test_a_delayed_epoch_write_held_after_its_read_expires_within_ttl_of_the_newer() -> None:
    stack = _stack()
    principal = await stack.principal()
    first_token = await stack.login(principal)
    assert await stack.verify(first_token) == "admitted"

    # The first logout's write-through reads epoch 1, then stalls before it writes.
    gate = stack.epochs.after_read = _Gate()
    first = asyncio.create_task(stack.logout(principal))
    await asyncio.wait_for(gate.reached.wait(), timeout=5)

    between = await stack.login(principal)
    assert await stack.verify(between) == "admitted"
    stack.clock.advance(timedelta(seconds=30))

    await stack.logout(principal)
    newer_commit = stack.clock.now
    stack.states.inner.state.revocation_states.clear()  # the newer epoch's key is lost
    stack.clock.advance(timedelta(seconds=10))

    gate.release.set()
    await first

    # The older epoch landed, with an expiry fixed before the newer commit.
    assert (await stack.states.inner.read(principal, "s:-")).epoch == "1"

    stack.clock.now = newer_commit + TTL.total_seconds()
    assert (await stack.states.inner.read(principal, "s:-")).epoch is None
    assert await stack.verify(between) == "session_revoked"


async def test_a_write_through_that_fails_every_attempt_is_logged_and_bounded_by_ttl() -> None:
    stack = _stack(write_attempts=2)
    principal = await stack.principal()
    issued = await stack.login(principal)
    assert await stack.verify(issued) == "admitted"

    stack.states.writes_down = True
    stack.states.calls.clear()

    with structlog.testing.capture_logs() as logs:
        await stack.logout(principal)  # the operation succeeds

    assert stack.states.calls == ["now", "now"]
    assert [entry["log_level"] for entry in logs if "write-through" in entry["event"]] == ["error"]

    stack.states.writes_down = False
    assert await stack.verify_without_reads(issued) == "admitted"  # the cached states are stale

    stack.clock.advance(TTL)
    assert await stack.verify(issued) == "session_revoked"


async def test_issuing_or_revoking_outside_a_transaction_is_refused_before_any_write() -> None:
    stack = _stack()
    principal = await stack.principal()
    issued = await stack.login(principal)
    lifecycle = stack.lifecycle
    calls: list[Callable[[], Awaitable[Any]]] = [
        lambda: lifecycle.issue_tokens(AuthnIdentity(principal_id=principal)),
        lambda: lifecycle.refresh_tokens(
            RefreshTokenCredentials(token=issued.refresh.token.token)  # type: ignore[union-attr]
        ),
        lambda: lifecycle.revoke_tokens(AuthnIdentity(principal_id=principal)),
        lambda: lifecycle.revoke_chain_of_tokens(principal, _claims(issued)["fam"]),
        lambda: stack.ctx.authn.principal_deactivation(SPEC).deactivate(principal),
    ]

    for call in calls:
        with pytest.raises(CoreException) as caught:
            await call()

        assert caught.value.code == "authn_revocation_requires_transaction"

    row = await stack.ctx.doc.query(policy_principal_spec).get(principal)
    assert row.is_active
    assert await stack.verify(issued) == "admitted"


async def test_a_registry_without_a_transaction_route_is_refused() -> None:
    stack = _stack()
    principal = await stack.principal()
    issued = await stack.login(principal)

    with pytest.raises(CoreException) as refresh:
        await run_operation(
            build_authn_registry(SPEC, tx_route=None).freeze(),
            NS.key(AuthnKernelOp.REFRESH_TOKENS),
            AuthnRefreshRequestDTO(refresh_token=issued.refresh.token.token),  # type: ignore[union-attr]
            stack.ctx,
        )

    with (
        stack.ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=principal)),
        pytest.raises(CoreException) as switch,
    ):
        await run_operation(
            build_tenancy_registry(SPEC, tx_route=None).freeze(),
            NS.key(TenancyKernelOp.SWITCH_TENANT),
            TenantSwitchRequestDTO(id=uuid4()),
            stack.ctx,
        )

    assert refresh.value.code == switch.value.code == "authn_revocation_requires_transaction"


async def test_a_tenant_mismatch_is_refused_from_the_cached_state() -> None:
    stack = _stack()
    principal = await stack.principal()
    issued = await stack.login(principal, tenant=uuid4())
    assert await stack.verify(issued) == "admitted"

    key = f"s:{_claims(issued)['sid']}"
    stored = stack.states.inner.state.revocation_states[(STATES, str(principal), key)]
    stack.states.inner.state.revocation_states[(STATES, str(principal), key)] = (
        f"live|{uuid4()}",
        stored[1],
    )

    assert await stack.verify_without_reads(issued) == "session_tenant_mismatch"


async def test_a_session_under_another_principal_is_refused_and_writes_nothing() -> None:
    stack = _stack()
    owner, intruder = await stack.principal(), await stack.principal()
    issued = await stack.login(owner)
    sid = _claims(issued)["sid"]

    async with stack.ctx.tx_ctx.scope(TX):
        foreign = await stack.lifecycle.issue_tokens(AuthnIdentity(principal_id=intruder))

    # A token for the intruder naming the owner's session, as a forged one would.
    forged = _resign(foreign, {**_claims(foreign), "sid": sid})
    before = dict(stack.states.inner.state.revocation_states)

    assert await stack.verify(forged) == "session_subject_mismatch"
    assert stack.states.inner.state.revocation_states == before


@pytest.mark.parametrize("value", ["live|not-a-uuid", "alive", "", "live"])
async def test_a_malformed_state_is_a_miss(value: str) -> None:
    stack = _stack()
    principal = await stack.principal()
    issued = await stack.login(principal)
    key = f"s:{_claims(issued)['sid']}"
    expire_at_ms = await stack.states.inner.now_ms(principal) + 60_000
    await stack.states.inner.fill(principal, {key: value}, expire_at_ms=expire_at_ms)

    stack.reads.clear()
    assert await stack.verify(issued) == "admitted"
    assert "session.find" in stack.reads

    await stack.revoke_family(principal, issued)
    assert await stack.verify(issued) == "session_revoked"


async def test_a_token_from_before_the_fast_path_is_checked_against_its_session_row() -> None:
    stack = _stack()
    principal = await stack.principal()
    issued = await stack.login(principal)
    claims = {k: v for k, v in _claims(issued).items() if k not in ("fam", "pep")}
    earlier = _resign(issued, claims)

    stack.reads.clear()
    assert await stack.verify(earlier) == "admitted"
    assert stack.reads == ["session.find"]
    assert stack.states.calls == []

    await stack.revoke_family(principal, issued)

    assert await stack.verify(earlier) == "session_revoked"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"ttl": timedelta(0)},
        {"write_attempts": 0},
        {"timeout": timedelta(0)},
        {"session_binding": "device"},
    ],
)
def test_a_configuration_that_cannot_bound_the_window_is_refused(kwargs: dict[str, Any]) -> None:
    with pytest.raises(CoreException) as caught:
        RevocationStates(**{"route": STATES, "ttl": TTL, **kwargs})

    assert caught.value.kind.value == "configuration"


async def test_a_fill_that_fails_admits_and_reads_the_database_next_time() -> None:
    stack = _stack()
    issued = await stack.login(await stack.principal())

    stack.states.fill_down = True
    assert await stack.verify(issued) == "admitted"

    stack.states.fill_down = False
    stack.reads.clear()
    assert await stack.verify(issued) == "admitted"
    assert stack.reads != []


@pytest.mark.parametrize(
    "claims",
    [{"pep": -1}, {"pep": True}, {"pep": "1"}, {"fam": "not-a-uuid"}, {"fam": 7}],
)
async def test_a_malformed_family_or_epoch_claim_is_an_invalid_token(
    claims: dict[str, Any],
) -> None:
    stack = _stack()
    issued = await stack.login(await stack.principal())

    assert await stack.verify(_resign(issued, {**_claims(issued), **claims})) == (
        "invalid_access_token"
    )


def test_a_verifier_with_revocation_states_needs_the_session_port() -> None:
    stack = _stack()
    revocation = stack.ctx.authn.token_lifecycle(SPEC).revocation

    with pytest.raises(CoreException):
        ForzeJwtTokenVerifier(
            access_svc=AccessTokenService(signer=Hs256Signer(secret=b"k" * 32)),
            revocation=revocation,
        )


# ....................... #


async def _logout_whose_write_is_lost(stack: _Stack, principal: UUID) -> None:
    stack.states.writes_down = True
    await stack.logout(principal)
    stack.states.writes_down = False


async def _mint(stack: _Stack, principal: UUID, what: str, token: IssuedTokens) -> Any:
    """One request: authenticate with *token*, then run the kit operation that mints from it."""

    async def request() -> Any:
        assert await stack.verify_here(token) == "admitted"

        with stack.ctx.inv_ctx.bind_identity(authn=AuthnIdentity(principal_id=principal)):
            if what == "switch":
                return await run_operation(
                    build_tenancy_registry(SPEC).freeze(),
                    NS.key(TenancyKernelOp.SWITCH_TENANT),
                    TenantSwitchRequestDTO(id=uuid4()),
                    stack.ctx,
                )

            return await run_operation(
                build_authn_registry(SPEC).freeze(),
                NS.key(AuthnKernelOp.ISSUE_API_KEY),
                AuthnIssueApiKeyRequestDTO(label="k"),
                stack.ctx,
            )

    return await asyncio.create_task(request())


@BINDINGS
@pytest.mark.parametrize("what", ["switch", "api_key"])
@pytest.mark.parametrize("revocation", ["logout", "family"])
async def test_a_token_admitted_inside_the_window_mints_nothing(
    binding: SessionBinding,
    what: str,
    revocation: str,
) -> None:
    stack = _stack(binding=binding)
    principal = await stack.principal()
    stolen = await stack.login(principal)
    assert await stack.verify(stolen) == "admitted"

    # A logout raises the epoch; a family revocation leaves it, so only the state refuses.
    stack.states.writes_down = True

    if revocation == "logout":
        await stack.logout(principal)

    else:
        await stack.revoke_family(principal, stolen)

    stack.states.writes_down = False

    # The cached states still admit it, as the window allows; minting from it must not.
    assert await stack.verify(stolen) == "admitted"

    with pytest.raises(CoreException) as caught:
        await _mint(stack, principal, what, stolen)

    assert caught.value.code == "session_revoked"


@pytest.mark.parametrize("what", ["switch", "api_key"])
async def test_a_live_token_still_mints(what: str) -> None:
    stack = _stack()
    principal = await stack.principal()
    issued = await stack.login(principal)
    assert await stack.verify(issued) == "admitted"

    assert await _mint(stack, principal, what, issued) is not None


async def test_a_password_login_after_a_stale_token_is_not_refused() -> None:
    stack = _stack()
    principal = await stack.principal()
    await stack.ctx.authn.password_account_provisioning(SPEC).register_with_password(
        principal,
        PasswordCredentials(login="alice", password="pw-1"),
    )
    stale = await stack.login(principal)
    assert await stack.verify(stale) == "admitted"
    await _logout_whose_write_is_lost(stack, principal)
    assert await stack.verify(stale) == "admitted"

    # One request presents the stale token and logs in with a password: the password is the
    # credential it authenticated with last, so the issue does not re-check the token.
    async def request() -> Any:
        assert await stack.verify_here(stale) == "admitted"

        return await run_operation(
            build_authn_registry(SPEC).freeze(),
            NS.key(AuthnKernelOp.PASSWORD_LOGIN),
            AuthnLoginRequestDTO(login="alice", password="pw-1"),
            stack.ctx,
        )

    assert await asyncio.create_task(request()) is not None


async def _mint_for(
    stack: _Stack, caller: UUID, other: UUID, mint: str, token: IssuedTokens
) -> Any:
    """One request: authenticate with *token* as *caller*, then mint a credential for *other*."""

    operator = AuthnIdentity(principal_id=caller)
    provisioning = stack.ctx.authn.password_account_provisioning(SPEC)

    async def request() -> Any:
        assert await stack.verify_here(token) == "admitted"

        if mint == "session":
            # As when a resolver maps the token's subject to another principal.
            return await stack.login(other)

        async with stack.ctx.tx_ctx.scope(TX):
            if mint == "invite":
                return await provisioning.issue_password_invite(operator, other)

            await provisioning.provision_password_account(
                operator, other, PasswordCredentials(login=f"{other}", password="pw-1")
            )
            return "provisioned"

    return await asyncio.create_task(request())


MINTS_FOR_ANOTHER = pytest.mark.parametrize("mint", ["session", "invite", "provision"])


@MINTS_FOR_ANOTHER
async def test_a_live_caller_still_mints_for_another_subject(mint: str) -> None:
    stack = _stack()
    caller, other = await stack.principal(), await stack.principal()
    token = await stack.login(caller)

    assert await _mint_for(stack, caller, other, mint, token) is not None


@BINDINGS
@MINTS_FOR_ANOTHER
async def test_a_caller_admitted_inside_the_window_mints_nothing_for_another_subject(
    binding: SessionBinding,
    mint: str,
) -> None:
    # The caller's credential is what must still be live, whoever the credential is for.
    stack = _stack(binding=binding)
    caller, other = await stack.principal(), await stack.principal()
    stolen = await stack.login(caller)
    assert await stack.verify(stolen) == "admitted"
    await _logout_whose_write_is_lost(stack, caller)
    assert await stack.verify(stolen) == "admitted"

    with pytest.raises(CoreException) as caught:
        await _mint_for(stack, caller, other, mint, stolen)

    assert caught.value.code == "session_revoked"
