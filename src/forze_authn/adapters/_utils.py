from uuid import UUID

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.document import DocumentQueryPort
from forze.base.errors import AuthenticationError

from ..domain.models.account import (
    ReadApiKeyAccount,
    ReadPasswordAccount,
    ReadPrincipal,
)

# ----------------------- #


async def find_password_account_by_login(
    qry: DocumentQueryPort[ReadPasswordAccount],
    login: str,
) -> ReadPasswordAccount | None:
    return await qry.find(
        filters={
            "$or": [
                {"$fields": {"username": login}},
                {"$fields": {"email": login}},
            ]
        }
    )


# ....................... #


async def find_password_account_by_authn_identity(
    qry: DocumentQueryPort[ReadPasswordAccount],
    identity: AuthnIdentity,
) -> ReadPasswordAccount | None:
    return await qry.find(
        filters={
            "$fields": {
                "principal_id": identity.principal_id,
            },
        }
    )


# ....................... #


async def find_api_key_account_by_key_hash(
    qry: DocumentQueryPort[ReadApiKeyAccount],
    key_hash: str,
) -> ReadApiKeyAccount | None:
    return await qry.find(
        filters={
            "$fields": {
                "key_hash": key_hash,
            },
        }
    )


# ....................... #


async def find_api_key_account_by_authn_identity(
    qry: DocumentQueryPort[ReadApiKeyAccount],
    identity: AuthnIdentity,
) -> ReadApiKeyAccount | None:
    return await qry.find(
        filters={
            "$fields": {
                "principal_id": identity.principal_id,
            },
        }
    )


# ....................... #
#! TODO: repurpose into abstract port which by id gives ReadPrincipal or None


async def validate_principal(
    qry: DocumentQueryPort[ReadPrincipal],
    principal_id: UUID,
) -> None:
    principal = await qry.find(
        filters={
            "$fields": {
                "id": principal_id,
            },
        }
    )

    if principal is None or not principal.is_active:
        raise AuthenticationError("Principal not found")
