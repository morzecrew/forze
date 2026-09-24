"""Claim ownership: whose key a claim is, and which invocation holds it in progress."""

from collections.abc import Callable
from uuid import UUID

import attrs

from ..authn import AuthnIdentity

# ----------------------- #


# Non-slotted on purpose: every idempotency store already inherits ``TenancyMixin`` (Redis
# through ``RedisBaseAdapter``), and two slotted bases are a C-level instance lay-out
# conflict. The cost is a ``__dict__`` on a store built once per invocation, next to nothing
# beside the round trip it is about to make; the alternative is this field copied into four
# adapters, where the semantics below would drift apart one adapter at a time.
@attrs.define(slots=False, kw_only=True, frozen=True)
class ClaimOwnerMixin:
    """Mixin giving a store the identity of the invocation taking a claim.

    ``begin`` / ``commit`` / ``fail`` identify a claim by ``op``, key and payload hash —
    three values that **two duplicates of one request necessarily share**. When an
    operation overruns its dedup window and a duplicate reclaims the key, the first
    operation's late ``commit`` matches the duplicate's live claim on every predicate the
    port's signature permits, and overwrites it: two executions, one cached result, and a
    record describing whichever committed first rather than the one whose effects survived.

    The owner is what separates them. It is the invocation's ``execution_id``, delivered
    the way the tenant already is — a callable injected at wiring, so no port signature
    changes — and each store writes it into the claim and adds it to the predicate
    ``commit`` and ``fail`` already use.

    Fencing is **conditional on both sides carrying an owner**: a store wired without a
    provider, or a claim written before this existed, keeps the previous behaviour rather
    than refusing work. That is what makes the field additive; :meth:`claim_owner`
    returning ``None`` is the degraded path, not an error.
    """

    owner_provider: Callable[[], UUID | None] | None = attrs.field(default=None)
    """Callable yielding the current invocation's id (wired like ``tenant_provider``)."""

    # ....................... #

    def claim_owner(self) -> UUID | None:
        """The invocation id to fence this store's claims on, if one is available.

        ``None`` when no provider is wired or the call happens outside an invocation —
        both of which degrade to unfenced behaviour rather than failing, since a store
        that refused to work without an ambient invocation would break every direct
        construction (tests, the oracle under DST, offline tooling).
        """

        if self.owner_provider is None:
            return None

        return self.owner_provider()


# ....................... #


def scoped_claim_key(principal_id: UUID | None, key: str) -> str:
    """The key a claim is stored under: the caller's *key*, scoped to who is acting.

    A client generates its idempotency key, so nothing makes it unique across callers, and two
    principals who pick the same one must never meet — the second would be served the first's
    result. So the principal is part of the stored key, and **every** key is scoped: one with
    no principal carries a marker of its own rather than going in raw, because a raw key could
    be chosen to equal another principal's scoped one.

    The two forms are told apart by their first character, and a principal's id has a fixed
    length, so nothing the caller puts in *key* can reach into the part that says whose it is.
    """

    if principal_id is None:
        return f"a:{key}"

    return f"p:{principal_id}:{key}"


# ....................... #


# Non-slotted for the same reason as ``ClaimOwnerMixin``.
@attrs.define(slots=False, kw_only=True, frozen=True)
class ClaimPrincipalMixin:
    """Mixin scoping a store's claims to the principal the operation acts for.

    ``begin`` / ``commit`` / ``fail`` identify a claim by ``op``, key and payload hash, none of
    which says *whose* key it is. Two principals in one tenant submitting the same key would
    share one claim: with different arguments the second gets ``conflict`` — which tells it the
    key is someone else's — and with identical arguments it is served the first principal's
    stored result.

    The principal is delivered the way the tenant and the owner are — a callable injected at
    wiring, so no port signature changes — and each store passes the caller's key through
    :meth:`claim_key` before using it, so a key in use by another principal behaves as unused.

    Unlike the owner fence this does **not** degrade to the previous behaviour when nothing is
    wired: an unscoped claim is exactly the defect. A store without a provider, or a call with
    no authenticated principal, scopes its keys to the anonymous space, which no principal's
    claim ever matches.
    """

    principal_provider: Callable[[], AuthnIdentity | None] | None = attrs.field(default=None)
    """Callable yielding the current invocation's identity (wired like ``tenant_provider``)."""

    # ....................... #

    def claim_key(self, key: str) -> str:
        """*key* as this store holds it, scoped to the principal acting now.

        Keyed on the **subject** (``principal_id``), not the actor of a delegated call: an
        idempotency key protects the effect on the subject's data, so an agent retrying a
        user's request replays it rather than running it again.
        """

        identity = self.principal_provider() if self.principal_provider is not None else None

        return scoped_claim_key(identity.principal_id if identity is not None else None, key)
