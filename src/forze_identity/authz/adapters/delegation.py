"""Pairwise delegation (``may_act``) grants backed by junction documents."""

from typing import Any, final
from uuid import UUID

import attrs

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import (
    AuthzScope,
    AuthzSubject,
    DelegationGrantPort,
    DelegationPort,
    PrincipalRef,
    resolve_policy_scope,
    subject_for_grant_query,
)
from forze.application.contracts.authz.specs import AuthzSpec
from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.base.exceptions import exc

from ..domain.models.bindings import (
    CreateDelegationGrantCmd,
    DelegationGrant,
    ReadDelegationGrant,
)
from ..domain.models.policy_principal import ReadPolicyPrincipal
from ..services.grants import fetch_all_document_hits
from ._utils import find_policy_principal_by_id, validate_authz_query_ports

# ----------------------- #


def _invocation_tenant(scope: AuthzScope | None) -> UUID | None:
    return scope.tenant_id if scope is not None else None


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DelegationQueryAdapter(DelegationPort):
    """Answer ``may_act(actor, subject)`` from delegation junction documents."""

    spec: AuthzSpec
    grant_qry: DocumentQueryPort[ReadDelegationGrant]

    # ....................... #

    def __attrs_post_init__(self) -> None:
        validate_authz_query_ports(self.spec, (self.grant_qry,))

    # ....................... #

    async def may_act(
        self,
        actor_id: UUID,
        subject_id: UUID,
        *,
        scope: AuthzScope | None = None,
    ) -> bool:
        _ = resolve_policy_scope(
            spec=self.spec,
            explicit=scope,
            invocation_tenant_id=_invocation_tenant(scope),
        )
        row = await self.grant_qry.find(
            filters={"$values": {"actor_id": actor_id, "subject_id": subject_id}},
        )

        return row is not None


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DelegationGrantAdapter(DelegationGrantPort):
    """Attach and list delegation grants backed by junction documents."""

    spec: AuthzSpec
    principal_qry: DocumentQueryPort[ReadPolicyPrincipal]
    grant_qry: DocumentQueryPort[ReadDelegationGrant]
    grant_cmd: DocumentCommandPort[
        ReadDelegationGrant,
        DelegationGrant,
        CreateDelegationGrantCmd,
        Any,
    ]

    # ....................... #

    def __attrs_post_init__(self) -> None:
        validate_authz_query_ports(self.spec, (self.principal_qry, self.grant_qry))

    # ....................... #

    async def grant_delegation(
        self,
        actor: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        *,
        scope: AuthzScope | None = None,
    ) -> None:
        _ = resolve_policy_scope(
            spec=self.spec,
            explicit=scope,
            invocation_tenant_id=_invocation_tenant(scope),
        )
        actor_id = subject_for_grant_query(actor)
        subject_id = subject_for_grant_query(subject)

        await self._require_principal(actor_id, "delegation actor")
        await self._require_principal(subject_id, "delegation subject")

        existing = await self._find_grant(actor_id, subject_id)

        if existing is not None:
            return

        await self.grant_cmd.create(
            CreateDelegationGrantCmd(actor_id=actor_id, subject_id=subject_id),
            return_new=False,
        )

    # ....................... #

    async def revoke_delegation(
        self,
        actor: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        subject: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        *,
        scope: AuthzScope | None = None,
    ) -> None:
        _ = resolve_policy_scope(
            spec=self.spec,
            explicit=scope,
            invocation_tenant_id=_invocation_tenant(scope),
        )
        actor_id = subject_for_grant_query(actor)
        subject_id = subject_for_grant_query(subject)

        grant = await self._find_grant(actor_id, subject_id)

        if grant is None:
            return

        await self.grant_cmd.kill(grant.id)

    # ....................... #

    async def list_delegators(
        self,
        actor: PrincipalRef | UUID | AuthnIdentity | AuthzSubject,
        *,
        scope: AuthzScope | None = None,
    ) -> frozenset[UUID]:
        _ = resolve_policy_scope(
            spec=self.spec,
            explicit=scope,
            invocation_tenant_id=_invocation_tenant(scope),
        )
        actor_id = subject_for_grant_query(actor)
        rows = await fetch_all_document_hits(
            self.grant_qry,
            filters={"$values": {"actor_id": actor_id}},
        )

        return frozenset(row.subject_id for row in rows)

    # ....................... #

    async def _require_principal(self, principal_id: UUID, label: str) -> None:
        principal_row = await find_policy_principal_by_id(self.principal_qry, principal_id)

        if principal_row is None:
            raise exc.internal(f"Policy principal not found for {label}")

    # ....................... #

    async def _find_grant(
        self,
        actor_id: UUID,
        subject_id: UUID,
    ) -> ReadDelegationGrant | None:
        return await self.grant_qry.find(
            filters={"$values": {"actor_id": actor_id, "subject_id": subject_id}},
        )
