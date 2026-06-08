from typing import final
from uuid import UUID

import attrs

from forze.application.contracts.authz import PrincipalKind, PrincipalRef, PrincipalRegistryPort
from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort

from ..domain.models.policy_principal import (
    CreatePolicyPrincipalCmd,
    PolicyPrincipal,
    ReadPolicyPrincipal,
    UpdatePolicyPrincipalCmd,
)
from ._utils import find_policy_principal_by_id, validate_secure_authz_document_spec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PrincipalRegistryAdapter(PrincipalRegistryPort):
    """Document-backed principal registry."""

    principal_qry: DocumentQueryPort[ReadPolicyPrincipal]
    principal_cmd: DocumentCommandPort[
        ReadPolicyPrincipal,
        PolicyPrincipal,
        CreatePolicyPrincipalCmd,
        UpdatePolicyPrincipalCmd,
    ]

    # ....................... #

    def __attrs_post_init__(self) -> None:
        validate_secure_authz_document_spec(self.principal_qry.spec)
        validate_secure_authz_document_spec(self.principal_cmd.spec)

    # ....................... #

    async def ensure_principal(
        self,
        principal_id: UUID,
        kind: PrincipalKind,
        *,
        is_active: bool = True,
    ) -> PrincipalRef:
        existing = await find_policy_principal_by_id(self.principal_qry, principal_id)

        if existing is not None:
            return PrincipalRef(
                principal_id=existing.id,
                kind=existing.kind,
                is_active=existing.is_active,
            )

        dto = CreatePolicyPrincipalCmd(kind=kind)
        created = await self.principal_cmd.create(dto, id=principal_id, return_new=True)

        if not is_active:
            await self.principal_cmd.update(
                created.id,
                created.rev,
                UpdatePolicyPrincipalCmd(is_active=False),
            )
            created = await self.principal_qry.get(created.id)

        return PrincipalRef(
            principal_id=created.id,
            kind=created.kind,
            is_active=created.is_active,
        )

    # ....................... #

    async def create_principal(self, kind: PrincipalKind) -> PrincipalRef:
        dto = CreatePolicyPrincipalCmd(kind=kind)
        created = await self.principal_cmd.create(dto, return_new=True)

        return PrincipalRef(
            principal_id=created.id,
            kind=created.kind,
            is_active=created.is_active,
        )

    # ....................... #

    async def get_principal(self, principal_id: UUID) -> PrincipalRef | None:
        row = await find_policy_principal_by_id(self.principal_qry, principal_id)

        if row is None:
            return None

        return PrincipalRef(
            principal_id=row.id,
            kind=row.kind,
            is_active=row.is_active,
        )

    # ....................... #

    async def deactivate_principal(self, principal_id: UUID) -> None:
        row = await find_policy_principal_by_id(self.principal_qry, principal_id)

        if row is None:
            return

        await self.principal_cmd.update(
            row.id,
            row.rev,
            UpdatePolicyPrincipalCmd(is_active=False),
        )
