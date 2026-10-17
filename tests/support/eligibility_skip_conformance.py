"""Shared battery for the eligibility skip and the deactivation writes it rests on.

The eligibility read after a token verification is skipped only for a non-locking check of the
admitted token's own subject, right after the fast path admitted it, in the same task. That is
sound only because every write that may deactivate a policy principal raises the principal's
epoch in its transaction, through the document's command port, whichever path writes it.

The deactivation cases run on the mock stores and on Postgres and Redis, since the hook sits on
the command port each backend builds. The skip cases run on Postgres only: the skip is refused
on any other backend, the mock included, whose locking read does not exclude a deactivation.

Used by:

- ``tests/unit/test_forze_identity/authn/test_eligibility_skip.py``
- ``tests/integration/test_forze_authn/test_pg_eligibility_skip.py``
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Protocol
from uuid import UUID

import attrs
import jwt

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    AuthnIdentity,
    AuthnSpec,
    IssuedTokens,
    PasswordCredentials,
    PrincipalEligibilityDepKey,
    TokenVerifierDepKey,
)
from forze.application.contracts.authz import AuthzSpec
from forze.application.contracts.deps.proxy import PortProxy
from forze.application.contracts.document import KeyedUpdate, UpsertItem
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze_identity.authz.application.specs import policy_principal_spec
from forze_identity.authz.domain.models.policy_principal import (
    CreatePolicyPrincipalCmd,
    UpdatePolicyPrincipalCmd,
)

# ----------------------- #


@attrs.define(slots=True)
class CountingPrincipalReads(PortProxy):
    """The policy principal's query port, recording each read it serves.

    A port proxy, so the backend behind it is still the one the skip's backend check sees.
    """

    reads: list[str] = attrs.field(kw_only=True)

    def _should_wrap(self, name: str, attr: Any) -> bool:
        del attr
        return name in ("find", "get", "find_many")

    def _wrap_async(self, name: str, attr: Any) -> Any:
        async def counted(*args: Any, **kwargs: Any) -> Any:
            self.reads.append(name)
            return await attr(*args, **kwargs)

        return counted


class Stack(Protocol):
    """One backend's identity plane, with revocation states on the authn and authz kernels."""

    ctx: ExecutionContext
    spec: AuthnSpec
    authz_route: str
    principal_reads: list[str]

    async def principal(self) -> UUID: ...

    async def run[T](self, work: Callable[[], Awaitable[T]]) -> T: ...

    async def in_tx[T](self, work: Callable[[], Awaitable[T]]) -> T: ...

    async def epoch(self, principal: UUID) -> int: ...


# ....................... #


async def login(stack: Stack, principal: UUID) -> IssuedTokens:
    lifecycle = stack.ctx.authn.token_lifecycle(stack.spec)

    return await stack.in_tx(
        lambda: lifecycle.issue_tokens(AuthnIdentity(principal_id=principal)),
    )


async def verify(stack: Stack, issued: IssuedTokens) -> str:
    """``admitted`` or the refusal's code, from a request of its own (its own task)."""

    async def request() -> str:
        try:
            await stack.ctx.authn.authn(stack.spec).authenticate_with_token(issued.access.token)

        except CoreException as error:
            return str(error.code)

        return "admitted"

    return await asyncio.create_task(stack.run(request))


async def verify_token_only(stack: Stack, issued: IssuedTokens) -> None:
    """Run the token verifier alone, in the current task, leaving its admission unconsumed."""

    verifier = stack.ctx.deps.provide(TokenVerifierDepKey, route=stack.spec.name)(
        stack.ctx,
        stack.spec,
    )
    await verifier.verify_token(AccessTokenCredentials(token=issued.access.token.token))


async def check_eligibility(stack: Stack, principal: UUID, *, for_update: bool = False) -> int:
    """The policy principal reads one eligibility check made, in the current task."""

    eligibility = stack.ctx.deps.provide(PrincipalEligibilityDepKey, route=stack.spec.name)(
        stack.ctx,
        stack.spec,
    )
    before = len(stack.principal_reads)
    await eligibility.require_authentication_allowed(principal, for_update=for_update)

    return len(stack.principal_reads) - before


def _deactivate() -> UpdatePolicyPrincipalCmd:
    return UpdatePolicyPrincipalCmd(is_active=False)


def _by_id(principal: UUID) -> Any:
    return {"$values": {"id": principal}}


async def _rev(stack: Stack, principal: UUID) -> int:
    row = await stack.ctx.doc.query(policy_principal_spec).get(principal)
    return row.rev


# ....................... #
# Each write path that may deactivate a principal. Each runs inside the stack's transaction.


async def _registry(stack: Stack, principal: UUID) -> None:
    registry = stack.ctx.authz.principal_registry(AuthzSpec(name=stack.authz_route))
    await registry.deactivate_principal(principal)


async def _authn_deactivation(stack: Stack, principal: UUID) -> None:
    await stack.ctx.authn.principal_deactivation(stack.spec).deactivate(principal)


async def _update(stack: Stack, principal: UUID) -> None:
    command = stack.ctx.doc.command(policy_principal_spec)
    await command.update(principal, await _rev(stack, principal), _deactivate())


async def _update_many(stack: Stack, principal: UUID) -> None:
    command = stack.ctx.doc.command(policy_principal_spec)
    await command.update_many(
        [KeyedUpdate(id=principal, rev=await _rev(stack, principal), dto=_deactivate())],
        return_new=False,
    )


async def _update_matching(stack: Stack, principal: UUID) -> None:
    command = stack.ctx.doc.command(policy_principal_spec)
    assert await command.update_matching(_by_id(principal), _deactivate(), return_new=False) == 1


async def _update_matching_rows(stack: Stack, principal: UUID) -> None:
    command = stack.ctx.doc.command(policy_principal_spec)
    rows = await command.update_matching(_by_id(principal), _deactivate())
    assert [row.id for row in rows] == [principal]


async def _update_matching_strict(stack: Stack, principal: UUID) -> None:
    command = stack.ctx.doc.command(policy_principal_spec)
    assert (
        await command.update_matching_strict(_by_id(principal), _deactivate(), return_new=False)
        == 1
    )


async def _upsert(stack: Stack, principal: UUID) -> None:
    command = stack.ctx.doc.command(policy_principal_spec)
    await command.upsert(principal, CreatePolicyPrincipalCmd(kind="user"), _deactivate())


async def _upsert_many(stack: Stack, principal: UUID) -> None:
    command = stack.ctx.doc.command(policy_principal_spec)
    await command.upsert_many(
        [
            UpsertItem(
                id=principal, create=CreatePolicyPrincipalCmd(kind="user"), update=_deactivate()
            )
        ],
    )


async def _upsert_many_set_based(stack: Stack, principal: UUID) -> None:
    command = stack.ctx.doc.command(policy_principal_spec)
    await command.upsert_many(
        [
            UpsertItem(
                id=principal, create=CreatePolicyPrincipalCmd(kind="user"), update=_deactivate()
            )
        ],
        return_new=False,
        set_based=True,
    )


async def _kill(stack: Stack, principal: UUID) -> None:
    await stack.ctx.doc.command(policy_principal_spec).kill(principal)


async def _kill_many(stack: Stack, principal: UUID) -> None:
    await stack.ctx.doc.command(policy_principal_spec).kill_many([principal])


DEACTIVATIONS: Mapping[str, Callable[[Stack, UUID], Awaitable[None]]] = {
    "registry": _registry,
    "authn_deactivation": _authn_deactivation,
    "update": _update,
    "update_many": _update_many,
    "update_matching": _update_matching,
    "update_matching_rows": _update_matching_rows,
    "update_matching_strict": _update_matching_strict,
    "upsert": _upsert,
    "upsert_many": _upsert_many,
    "upsert_many_set_based": _upsert_many_set_based,
    "kill": _kill,
    "kill_many": _kill_many,
}
"""Every write path that may make a principal ineligible."""


# ----------------------- #
# The skip


async def a_fast_path_admission_skips_the_read(stack: Stack) -> None:
    principal = await stack.principal()
    issued = await login(stack, principal)

    for _ in range(3):
        stack.principal_reads.clear()
        assert await verify(stack, issued) == "admitted"
        assert stack.principal_reads == []


async def a_locking_check_reads(stack: Stack) -> None:
    principal = await stack.principal()
    issued = await login(stack, principal)

    async def request() -> int:
        await verify_token_only(stack, issued)
        return await check_eligibility(stack, principal, for_update=True)

    assert await stack.in_tx(request) == 1


async def a_check_of_another_principal_reads(stack: Stack) -> None:
    principal, other = await stack.principal(), await stack.principal()
    issued = await login(stack, principal)

    async def request() -> int:
        await verify_token_only(stack, issued)
        return await check_eligibility(stack, other)

    assert await stack.run(request) == 1


async def a_check_in_another_task_reads(stack: Stack) -> None:
    principal = await stack.principal()
    issued = await login(stack, principal)

    async def request() -> int:
        await verify_token_only(stack, issued)
        # A task spawned after the verification copies its context; it is not the request.
        return await asyncio.create_task(check_eligibility(stack, principal))

    assert await stack.run(request) == 1


async def only_the_check_right_after_the_admission_skips(stack: Stack) -> None:
    principal = await stack.principal()
    issued = await login(stack, principal)

    async def request() -> list[int]:
        await verify_token_only(stack, issued)
        return [await check_eligibility(stack, principal) for _ in range(2)]

    assert await stack.run(request) == [0, 1]


async def a_skipped_check_still_refuses_after_a_deactivation(stack: Stack) -> None:
    """The epoch the deactivation raised refuses the token before any eligibility check."""

    principal = await stack.principal()
    issued = await login(stack, principal)
    assert await verify(stack, issued) == "admitted"

    await stack.in_tx(lambda: _update(stack, principal))

    stack.principal_reads.clear()
    assert await verify(stack, issued) == "session_revoked"
    assert stack.principal_reads == []


def resign(issued: IssuedTokens, **changes: Any) -> IssuedTokens:
    """The same access token with *changes* to its claims (``None`` drops one), signed with
    the stacks' key."""

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


async def _reads_on(stack: Stack, issued: IssuedTokens) -> list[str]:
    stack.principal_reads.clear()
    assert await verify(stack, issued) == "admitted"
    return list(stack.principal_reads)


async def a_token_from_before_the_fast_path_reads(stack: Stack) -> None:
    issued = await login(stack, await stack.principal())
    assert await _reads_on(stack, resign(issued, fam=None, pep=None)) == ["find"]


async def a_token_ahead_of_the_epoch_reads(stack: Stack) -> None:
    # Admitted, since only an epoch behind refuses, but not on the principal's current epoch.
    issued = await login(stack, await stack.principal())
    assert await _reads_on(stack, resign(issued, pep=1)) == ["find"]


async def a_password_after_a_token_forgets_the_admission(stack: Stack) -> None:
    principal = await stack.principal()
    issued = await login(stack, principal)
    credentials = PasswordCredentials(login=f"{principal}@example.com", password="horse battery")
    provisioning = stack.ctx.authn.password_account_provisioning(stack.spec)
    await stack.run(lambda: provisioning.register_with_password(principal, credentials))

    async def request() -> int:
        await verify_token_only(stack, issued)
        before = len(stack.principal_reads)
        # The orchestrator's own check, after the password, reads.
        await stack.ctx.authn.authn(stack.spec).authenticate_with_password(credentials)
        return len(stack.principal_reads) - before

    assert await asyncio.create_task(stack.run(request)) == 1


async def an_actor_check_reads_even_for_the_subject_itself(stack: Stack) -> None:
    """Needs a stack whose authn module reads the ``act`` claim."""

    principal = await stack.principal()
    issued = await login(stack, principal)

    # The subject's check skipped; the actor's, about the same principal, read.
    assert await _reads_on(stack, resign(issued, act={"sub": str(principal)})) == ["find"]


SKIP_CASES: Mapping[str, Callable[[Stack], Awaitable[None]]] = {
    "skips": a_fast_path_admission_skips_the_read,
    "locking": a_locking_check_reads,
    "another_principal": a_check_of_another_principal_reads,
    "another_task": a_check_in_another_task_reads,
    "consumed": only_the_check_right_after_the_admission_skips,
    "deactivated": a_skipped_check_still_refuses_after_a_deactivation,
    "token_before_fast_path": a_token_from_before_the_fast_path_reads,
    "token_ahead_of_epoch": a_token_ahead_of_the_epoch_reads,
    "password_after_token": a_password_after_a_token_forgets_the_admission,
}


# ----------------------- #
# The deactivation writes


async def a_deactivation_refuses_the_next_verification(stack: Stack, path: str) -> None:
    principal = await stack.principal()
    issued = await login(stack, principal)
    assert await verify(stack, issued) == "admitted"
    before = await stack.epoch(principal)

    await stack.in_tx(lambda: DEACTIVATIONS[path](stack, principal))

    assert await stack.epoch(principal) > before
    assert await verify(stack, issued) == "session_revoked"


async def a_deactivation_outside_a_transaction_writes_nothing(stack: Stack, path: str) -> None:
    principal = await stack.principal()
    issued = await login(stack, principal)
    before = await stack.epoch(principal)

    try:
        await stack.run(lambda: DEACTIVATIONS[path](stack, principal))

    except CoreException as error:
        assert error.code == "authn_revocation_requires_transaction", error.code

    else:
        raise AssertionError(f"{path} deactivated outside a transaction")

    row = await stack.run(
        lambda: stack.ctx.doc.query(policy_principal_spec).find(filters=_by_id(principal))
    )
    assert row is not None and row.is_active
    assert await stack.epoch(principal) == before
    assert await verify(stack, issued) == "admitted"


async def an_activation_raises_nothing(stack: Stack) -> None:
    principal = await stack.principal()
    issued = await login(stack, principal)
    before = await stack.epoch(principal)
    command = stack.ctx.doc.command(policy_principal_spec)

    async def touch_it() -> None:
        rev = await _rev(stack, principal)
        await command.update(principal, rev, UpdatePolicyPrincipalCmd(is_active=True))

    # Outside a transaction too: a write that deactivates nobody is not refused.
    await stack.run(touch_it)

    assert await stack.epoch(principal) == before
    assert await verify(stack, issued) == "admitted"
