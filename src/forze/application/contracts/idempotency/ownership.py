"""Claim ownership: which invocation holds an in-progress idempotency claim."""

from collections.abc import Callable
from uuid import UUID

import attrs

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
