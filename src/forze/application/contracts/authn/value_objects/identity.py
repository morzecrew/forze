from __future__ import annotations

from uuid import UUID

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AuthnIdentity:
    """Canonical authenticated subject inside Forze.

    Resolved from a :class:`~forze.application.contracts.authn.value_objects.assertion.VerifiedAssertion`
    by a :class:`~forze.application.contracts.authn.ports.resolution.PrincipalResolverPort`.
    Downstream consumers (use cases, guards, document/tenancy layers) only see this
    principal-only shape. Effective tenant context lives separately on
    :class:`~forze.application.contracts.tenancy.value_objects.TenantIdentity`.
    """

    principal_id: UUID
    """Internal principal identifier; aligns with :class:`~forze.application.contracts.authz.value_objects.PrincipalRef`."""

    actor: AuthnIdentity | None = None
    """The principal *performing* the action when this identity is acted-for on behalf of
    another (delegation / RFC 8693 ``act``). ``principal_id`` is the effective **subject**
    (e.g. the user); ``actor`` is the **agent** doing it. ``None`` means a direct,
    non-delegated call. Chainable for multi-hop delegation."""

    @property
    def is_delegated(self) -> bool:
        """Whether this identity is acting on behalf of another (an actor is attached)."""

        return self.actor is not None

    @property
    def performer_id(self) -> UUID:
        """The principal performing the call: the nearest actor when delegated, else itself."""

        return self.actor.principal_id if self.actor is not None else self.principal_id

    @property
    def actor_ids(self) -> tuple[UUID, ...]:
        """The delegation chain, nearest actor first; empty for a direct call."""

        chain: list[UUID] = []
        actor = self.actor

        while actor is not None:
            chain.append(actor.principal_id)
            actor = actor.actor

        return tuple(chain)
