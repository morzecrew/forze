from typing import Any
from uuid import UUID

from forze.application.contracts.document import DocumentQueryPort, DocumentSpec
from forze.base.errors import CoreError

from ..domain.models.policy_principal import ReadPolicyPrincipal

# ----------------------- #


def validate_secure_authz_document_spec(spec: DocumentSpec[Any, Any, Any, Any]) -> None:
    """Reject cache/history on authz documents (same rationale as authn principal docs)."""

    if spec.cache is not None:
        raise CoreError("Authz document caching is forbidden by security reasons")

    if spec.history_enabled:
        raise CoreError("Authz document history is forbidden by security reasons")


validate_policy_principal_spec = validate_secure_authz_document_spec


# ....................... #


async def find_policy_principal_by_id(
    qry: DocumentQueryPort[ReadPolicyPrincipal],
    principal_id: UUID,
) -> ReadPolicyPrincipal | None:
    """Load policy principal by document id."""

    return await qry.find(
        filters={
            "$fields": {
                "id": principal_id,
            },
        },
    )
