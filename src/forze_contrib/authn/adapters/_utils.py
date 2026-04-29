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
