from uuid import UUID

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.document import DocumentQueryPort

from ..domain.models.account import ReadApiKeyAccount, ReadPasswordAccount

# ----------------------- #


async def find_password_account_by_login(
    qry: DocumentQueryPort[ReadPasswordAccount],
    login: str,
) -> ReadPasswordAccount | None:
    return await qry.find(
        filters={
            "$or": [
                {"$values": {"username": login}},
                {"$values": {"email": login}},
            ]
        }
    )


# ....................... #


async def find_password_account_by_principal_id(
    qry: DocumentQueryPort[ReadPasswordAccount],
    principal_id: UUID,
) -> ReadPasswordAccount | None:
    return await qry.find(
        filters={
            "$values": {
                "principal_id": principal_id,
            },
        }
    )


# ....................... #


async def find_password_account_by_authn_identity(
    qry: DocumentQueryPort[ReadPasswordAccount],
    identity: AuthnIdentity,
) -> ReadPasswordAccount | None:
    return await find_password_account_by_principal_id(qry, identity.principal_id)


# ....................... #


async def find_api_key_account_by_key_hash(
    qry: DocumentQueryPort[ReadApiKeyAccount],
    key_hash: str,
) -> ReadApiKeyAccount | None:
    return await qry.find(
        filters={
            "$values": {
                "key_hash": key_hash,
            },
        }
    )


# ....................... #


async def find_api_key_account_by_id(
    qry: DocumentQueryPort[ReadApiKeyAccount],
    key_id: UUID,
) -> ReadApiKeyAccount | None:
    return await qry.find(
        filters={
            "$values": {
                "id": key_id,
            },
        }
    )
