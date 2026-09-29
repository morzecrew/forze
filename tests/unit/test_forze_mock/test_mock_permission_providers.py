"""The mock's authz decision runs permission providers as the identity plane does.

Without them a mock-backed app decides on seeded grants alone, more permissively than the plane
it stands in for: a deactivated member's denial never reaches the decision.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.contracts.authz import (
    AuthzRequest,
    AuthzSpec,
    AuthzSubject,
    DerivedPermissions,
)
from forze.base.exceptions import CoreException
from forze.testing import context_from_modules
from forze_mock import MockDepsModule

pytestmark = pytest.mark.unit

MEMBER = uuid4()
SPEC = AuthzSpec(name="main")


@attrs.define(slots=True, kw_only=True)
class _Provider:
    name: str = "members"
    keys: frozenset[str] = frozenset({"ledger.write", "ledger.read"})
    granted: frozenset[str] = frozenset()
    denied: frozenset[str] = frozenset()
    delay: float = 0.0
    seen: list[Any] = attrs.field(factory=list)

    async def derive(self, principal_id: UUID, ctx: Any) -> DerivedPermissions:
        self.seen.append(ctx)
        await asyncio.sleep(self.delay)
        return DerivedPermissions(granted=self.granted, denied=self.denied)


async def _allowed(
    provider: _Provider, action: str, *, seeded: str | None = None, **module: Any
) -> bool:
    ctx = context_from_modules(MockDepsModule(permission_providers=(provider,), **module))
    decision = ctx.authz.decision(SPEC)

    if seeded is not None:
        decision.seed_grant(MEMBER, seeded)  # type: ignore[attr-defined]

    result = await decision.authorize(
        AuthzRequest(subject=AuthzSubject(principal_id=MEMBER), action=action)
    )

    return result.allowed


class TestTheMockRunsProviders:
    async def test_a_derived_denial_outranks_a_seeded_grant(self) -> None:
        provider = _Provider(denied=frozenset({"ledger.write"}))

        assert not await _allowed(provider, "ledger.write", seeded="ledger.write")

    async def test_a_derived_grant_counts_like_a_seeded_one(self) -> None:
        assert await _allowed(_Provider(granted=frozenset({"ledger.read"})), "ledger.read")

    async def test_a_provider_that_hangs_denies_what_it_declares(self) -> None:
        slow = _Provider(granted=frozenset({"ledger.read"}), delay=5)

        # Half the 2 s default: a deadline that is not wired through fails here, not at it.
        allowed = await asyncio.wait_for(
            _allowed(
                slow,
                "ledger.read",
                seeded="ledger.read",
                permission_provider_timeout=timedelta(milliseconds=20),
            ),
            timeout=1,
        )

        assert not allowed

    async def test_a_provider_reads_through_the_resolving_context(self) -> None:
        provider = _Provider()

        await _allowed(provider, "ledger.read")

        assert provider.seen and provider.seen[0] is not None

    async def test_without_providers_the_seeded_grants_decide(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        decision = ctx.authz.decision(SPEC)
        decision.seed_grant(MEMBER, "ledger.write")  # type: ignore[attr-defined]

        result = await decision.authorize(
            AuthzRequest(subject=AuthzSubject(principal_id=MEMBER), action="ledger.write")
        )

        assert result.allowed


class TestTheDeclarationIsChecked:
    """The mock refuses what the identity plane's configuration refuses, when it is built.

    A declaration the plane would refuse fails open in a decision instead: keys given as a bare
    string are denied letter by letter when the provider fails, and the seeded grant wins.
    """

    @pytest.mark.parametrize(
        "provider",
        [
            _Provider(keys="ledger.write"),  # type: ignore[arg-type]
            _Provider(keys=frozenset()),
            _Provider(name=" "),
        ],
        ids=["bare-string-keys", "no-keys", "blank-name"],
    )
    def test_a_malformed_provider_is_refused(self, provider: _Provider) -> None:
        with pytest.raises(CoreException) as caught:
            MockDepsModule(permission_providers=(provider,))

        assert caught.value.code == "authz_provider_declaration"

    @pytest.mark.parametrize("timeout", [timedelta(0), timedelta(seconds=-1)])
    def test_a_deadline_is_positive(self, timeout: timedelta) -> None:
        with pytest.raises(CoreException):
            MockDepsModule(permission_provider_timeout=timeout)
