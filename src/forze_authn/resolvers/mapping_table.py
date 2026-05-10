from typing import Any, final
from uuid import UUID, uuid4

import attrs

from forze.application.contracts.authn import (
    AuthnIdentity,
    PrincipalResolverPort,
    VerifiedAssertion,
)
from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.base.errors import AuthenticationError, CoreError

from ..domain.models.identity_mapping import (
    CreateIdentityMappingCmd,
    IdentityMapping,
    ReadIdentityMapping,
)

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MappingTableResolver(PrincipalResolverPort):
    """Resolve assertions via a document-backed ``(issuer, subject) -> principal_id`` registry.

    On a hit, returns the stored ``principal_id``. On a miss, behaviour depends on the
    ``provision_on_first_sight`` flag: when ``True``, mints a fresh :class:`UUID` and
    persists the mapping (typical for SSO-only deployments); when ``False``, raises
    :class:`AuthenticationError` (typical for invitation-only deployments where principal
    rows are pre-created and mapped out of band).

    The ``tenant_hint`` from the assertion is interpreted as a UUID when present; mapping
    rows do not carry tenant info today (left to per-deployment policy).
    """

    qry: DocumentQueryPort[ReadIdentityMapping]
    """Identity mapping query port."""

    cmd: DocumentCommandPort[
        ReadIdentityMapping,
        IdentityMapping,
        CreateIdentityMappingCmd,
        Any,
    ] | None = attrs.field(default=None)
    """Command port; required when ``provision_on_first_sight`` is ``True``."""

    provision_on_first_sight: bool = attrs.field(default=False)
    """Whether to mint a new principal id when the mapping is unknown."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        spec = self.qry.spec

        if spec.cache is not None:
            raise CoreError(
                "Identity mapping caching is forbidden by security reasons"
            )

        if spec.history_enabled:
            raise CoreError(
                "Identity mapping history is forbidden by security reasons"
            )

        if self.provision_on_first_sight and self.cmd is None:
            raise CoreError(
                "MappingTableResolver requires a command port to provision new mappings",
            )

    # ....................... #

    async def resolve(self, assertion: VerifiedAssertion) -> AuthnIdentity:
        existing = await self.qry.find(
            filters={
                "$fields": {
                    "issuer": assertion.issuer,
                    "subject": assertion.subject,
                }
            }
        )

        if existing is not None:
            return AuthnIdentity(
                principal_id=existing.principal_id,
                tenant_id=self._coerce_tenant(assertion),
            )

        if not self.provision_on_first_sight:
            raise AuthenticationError(
                "No identity mapping for this subject",
                code="unknown_external_subject",
            )

        if self.cmd is None:  # defensive; covered by post-init
            raise CoreError(
                "MappingTableResolver requires a command port to provision new mappings",
            )

        new_pid = uuid4()

        await self.cmd.create(
            CreateIdentityMappingCmd(
                issuer=assertion.issuer,
                subject=assertion.subject,
                principal_id=new_pid,
            ),
            return_new=False,
        )

        return AuthnIdentity(
            principal_id=new_pid,
            tenant_id=self._coerce_tenant(assertion),
        )

    # ....................... #

    @staticmethod
    def _coerce_tenant(assertion: VerifiedAssertion) -> UUID | None:
        if assertion.tenant_hint is None:
            return None

        try:
            return UUID(assertion.tenant_hint)

        except ValueError:
            return None
