"""Skipping the eligibility read after a fast-path admission, on the identity plane's adapters.

The adapters run over mock documents, the mock's journal transactions and the mock states and
epoch stores. The shared battery is the same one Postgres and Redis run; the cases here are the
configuration and credential cases that need the mock's flexibility.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, TypeVar
from uuid import UUID

import attrs
import jwt
import pytest

from forze.application.contracts.authn import (
    AuthnSpec,
    IssuedTokens,
    PasswordCredentials,
    PrincipalEpochDepKey,
    RevocationStatesDepKey,
)
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentCommandHookDepKey,
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
from forze_identity.authn.services import PasswordConfig
from forze_identity.authz import AuthzDepsModule, AuthzKernelConfig
from forze_identity.authz.application.specs import policy_principal_spec
from forze_identity.authz.domain.models.policy_principal import CreatePolicyPrincipalCmd
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


class _NotABackend:
    """A query port no backend module built: the skip's backend check refuses it."""

    def __init__(self, inner: Any, reads: list[str]) -> None:
        self._inner, self._reads = inner, reads

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._inner, name)

        if name != "find":
            return attr

        async def counted(*args: Any, **kwargs: Any) -> Any:
            self._reads.append(name)
            return await attr(*args, **kwargs)

        return counted


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
    actor_claim: str | None = None,
    backend: bool = True,
    extra: Deps | None = None,
) -> _Stack:
    mock = MockDepsModule()
    state = mock.state
    reads: list[str] = []
    document = ConfigurableMockDocument(module=mock)
    epochs = MockPrincipalEpochStore(state=state, in_transaction=lambda: False)
    states = MockRevocationStates(state=state, namespace=STATES)

    def principals(ctx: ExecutionContext, spec: Any) -> Any:
        if backend:
            return CountingPrincipalReads(document(ctx, spec), reads=reads)

        return _NotABackend(document(ctx, spec), reads)

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
            actor_claim=actor_claim,
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

    if extra is not None:
        deps = deps.merge(extra)

    return _Stack(ctx=context_from_deps(deps), principal_reads=reads, epochs=epochs)


def _resign(issued: IssuedTokens, **changes: Any) -> IssuedTokens:
    """The same access token with *changes* to its claims (``None`` drops one)."""

    claims = jwt.decode(issued.access.token.token, options={"verify_signature": False})

    for name, value in changes.items():
        if value is None:
            claims.pop(name, None)

        else:
            claims[name] = value

    token = jwt.encode(claims, b"k" * 32, algorithm="HS256")

    return IssuedTokens(
        access=attrs.evolve(issued.access, token=attrs.evolve(issued.access.token, token=token)),
        refresh=None,
    )


# ----------------------- #


@pytest.mark.parametrize("case", sorted(battery.SKIP_CASES))
async def test_the_skip(case: str) -> None:
    await battery.SKIP_CASES[case](_stack())


@pytest.mark.parametrize("path", sorted(battery.DEACTIVATIONS))
async def test_a_deactivation_refuses_the_next_verification(path: str) -> None:
    await battery.a_deactivation_refuses_the_next_verification(_stack(), path)


@pytest.mark.parametrize("path", sorted(battery.DEACTIVATIONS))
async def test_a_deactivation_outside_a_transaction_writes_nothing(path: str) -> None:
    await battery.a_deactivation_outside_a_transaction_writes_nothing(_stack(), path)


async def test_an_activation_raises_nothing() -> None:
    await battery.an_activation_raises_nothing(_stack())


# ....................... #


@pytest.mark.parametrize(
    "stack",
    [
        pytest.param(lambda: _stack(authz=None), id="no-hook"),
        pytest.param(lambda: _stack(authz=None, authn=None), id="no-fast-path"),
        pytest.param(lambda: _stack(backend=False), id="unsupported-backend"),
    ],
)
async def test_without_every_precondition_each_verification_reads(
    stack: Callable[[], _Stack],
) -> None:
    built = stack()
    issued = await battery.login(built, await built.principal())

    for _ in range(2):
        built.principal_reads.clear()
        assert await battery.verify(built, issued) == "admitted"
        assert built.principal_reads == ["find"]


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


async def test_another_hook_on_the_document_enables_no_skip() -> None:
    stack = _stack(
        authz=None,
        extra=Deps.routed(
            {DocumentCommandHookDepKey: {policy_principal_spec.name: lambda ctx, port: port}},
        ),
    )
    issued = await battery.login(stack, await stack.principal())

    stack.principal_reads.clear()
    assert await battery.verify(stack, issued) == "admitted"
    assert stack.principal_reads == ["find"]


async def test_a_token_from_before_the_fast_path_reads() -> None:
    stack = _stack()
    issued = _resign(await battery.login(stack, await stack.principal()), fam=None, pep=None)

    stack.principal_reads.clear()
    assert await battery.verify(stack, issued) == "admitted"
    assert stack.principal_reads == ["find"]


async def test_a_token_ahead_of_the_epoch_reads() -> None:
    # Admitted, since only an epoch behind refuses, but not on the principal's current epoch.
    stack = _stack()
    issued = await battery.login(stack, await stack.principal())
    ahead = _resign(issued, pep=1)

    stack.principal_reads.clear()
    assert await battery.verify(stack, ahead) == "admitted"
    assert stack.principal_reads == ["find"]


async def test_an_actor_check_reads_even_for_the_subject_itself() -> None:
    stack = _stack(actor_claim="act")
    principal = await stack.principal()
    issued = await battery.login(stack, principal)
    delegated = _resign(issued, act={"sub": str(principal)})

    stack.principal_reads.clear()
    assert await battery.verify(stack, delegated) == "admitted"
    # The subject's check skipped; the actor's, about the same principal, read.
    assert stack.principal_reads == ["find"]


async def test_a_password_login_after_a_token_forgets_the_admission() -> None:
    stack = _stack()
    principal = await stack.principal()
    issued = await battery.login(stack, principal)
    credentials = PasswordCredentials(login="user@example.com", password="correct horse battery")
    provisioning = stack.ctx.authn.password_account_provisioning(SPEC)
    await provisioning.register_with_password(principal, credentials)

    async def request() -> int:
        await battery.verify_token_only(stack, issued)
        verifier = stack.ctx.authn.authn(SPEC)
        stack.principal_reads.clear()
        # The orchestrator's own check, after the password, reads.
        await verifier.authenticate_with_password(credentials)
        return len(stack.principal_reads)

    assert await asyncio.create_task(request()) == 1
