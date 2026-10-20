from collections.abc import Callable
from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.authn import PrincipalEligibilityPort
from forze.application.contracts.document import DocumentQueryPort
from forze.base.exceptions import exc
from forze_identity._secure_spec import forbid_cache_and_history
from forze_identity.authz.domain.models.policy_principal import ReadPolicyPrincipal

from ..services import admitted_just_now

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PolicyPrincipalEligibilityAdapter(PrincipalEligibilityPort):
    """Require an active policy principal before authentication or credential mutation."""

    principal_qry: DocumentQueryPort[ReadPolicyPrincipal]
    """Query port for policy principals."""

    in_transaction: Callable[[], bool] | None = None
    """Whether a transaction is open. ``for_update`` locks only inside one: outside, there is
    no transaction to hold the lock and the backend refuses the read. ``None``, when the
    adapter is built without the probe, never locks. The deps module wires it."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        spec = self.principal_qry.spec

        forbid_cache_and_history(spec, label="Policy principal")

    # ....................... #

    async def require_authentication_allowed(
        self,
        principal_id: UUID,
        *,
        for_update: bool = False,
    ) -> None:
        principal = await self.principal_qry.find(
            filters={
                "$values": {
                    "id": principal_id,
                },
            },
            for_update=for_update and self.in_transaction is not None and self.in_transaction(),
        )

        if principal is None or not principal.is_active:
            raise exc.authentication("Principal not found")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AdmittedTokenEligibilityAdapter(PrincipalEligibilityPort):
    """Skip the eligibility read right after a token of the same principal was admitted.

    A token the revocation states admitted on its principal's current epoch was issued while
    the principal was eligible (an issue reads eligibility under the principal's row lock and
    holds the epoch), and every write that deactivates a principal raises its epoch, so
    nothing has deactivated it since, within the revocation states' window. An API key whose
    state recorded the principal's current epoch counts the same: the state was written once
    the principal was read active after that epoch. Only the call that
    follows that verification in the same task may rely on it, and only a non-locking one about
    the token's own subject: an issue locks and always reads, and an actor check comes after
    the subject's and always reads.
    """

    inner: PrincipalEligibilityPort
    """The eligibility read this skips."""

    # ....................... #

    async def require_authentication_allowed(
        self,
        principal_id: UUID,
        *,
        for_update: bool = False,
    ) -> None:
        # Consumed whatever this call asks, so no later call inherits the verification.
        if admitted_just_now(principal_id) and not for_update:
            return

        await self.inner.require_authentication_allowed(principal_id, for_update=for_update)


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class AllowAllPrincipalEligibilityAdapter(PrincipalEligibilityPort):
    """Every principal is eligible — the explicit opt-out of the policy-principal gate.

    For token-only deployments with no authz plane: the default
    :class:`PolicyPrincipalEligibilityAdapter` requires the ``policy_principal``
    document (and its upsert bookkeeping) to exist purely so authentication can
    check ``is_active``. A service that has no policy principals opts out here —
    a *declared* decision (``AuthnDepsModule(eligibility="allow_all")``), never a
    silent default: with it, deactivating a principal no longer blocks token
    issuance, so revocation must come from credential lifecycle (session revoke,
    API-key revoke) alone.
    """

    async def require_authentication_allowed(
        self,
        principal_id: UUID,
        *,
        for_update: bool = False,
    ) -> None:
        del principal_id, for_update
