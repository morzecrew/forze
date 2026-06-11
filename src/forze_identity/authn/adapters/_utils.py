from typing import Any, Mapping
from uuid import UUID

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.document import DocumentCommandPort, DocumentQueryPort
from forze.base.exceptions import exc
from forze.base.primitives import utcnow

from ..domain.models.account import ReadApiKeyAccount, ReadPasswordAccount
from ..domain.models.invite import ReadPasswordInvite
from ..domain.models.reset import ReadPasswordReset
from ..domain.models.session import (
    CreateSessionCmd,
    ReadSession,
    Session,
    UpdateSessionCmd,
)

# ----------------------- #


async def find_password_account_by_login(
    qry: DocumentQueryPort[ReadPasswordAccount],
    login: str,
) -> ReadPasswordAccount | None:
    page = await qry.find_many(
        filters={
            "$or": [
                {"$values": {"username": login}},
                {"$values": {"email": login}},
            ]
        },
        pagination={"limit": 2},
    )

    if not page.hits:
        return None

    if len(page.hits) > 1:
        raise exc.internal(
            "Multiple password accounts match this login",
            code="password_account_ambiguous",
        )

    return page.hits[0]


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


# ....................... #


async def revoke_sessions_matching(
    session_qry: DocumentQueryPort[ReadSession],
    session_cmd: DocumentCommandPort[
        ReadSession,
        Session,
        CreateSessionCmd,
        UpdateSessionCmd,
    ],
    values: Mapping[str, Any],
) -> None:
    """Revoke (set ``revoked_at``) every session matching the ``$values`` filter.

    Shared by cascaded principal deactivation (token lifecycle) and the
    password-change "log out everywhere" cascade.
    """

    sessions = await session_qry.find_many(
        filters={
            "$values": dict(values),
        }
    )

    upds = [(x.id, x.rev, UpdateSessionCmd(revoked_at=utcnow())) for x in sessions.hits]

    await session_cmd.update_many(upds, return_new=False)


# ....................... #


async def find_password_invite_by_digest(
    qry: DocumentQueryPort[ReadPasswordInvite],
    token_digest: str,
) -> ReadPasswordInvite | None:
    return await qry.find(
        filters={
            "$values": {
                "token_digest": token_digest,
            },
        }
    )


# ....................... #


async def find_password_reset_by_digest(
    qry: DocumentQueryPort[ReadPasswordReset],
    token_digest: str,
) -> ReadPasswordReset | None:
    return await qry.find(
        filters={
            "$values": {
                "token_digest": token_digest,
            },
        }
    )


# ....................... #


async def find_outstanding_password_resets(
    qry: DocumentQueryPort[ReadPasswordReset],
    principal_id: UUID,
) -> list[ReadPasswordReset]:
    """Find every still-unused reset for ``principal_id`` (for supersession)."""

    page = await qry.find_many(
        filters={
            "$values": {
                "principal_id": principal_id,
                "used_at": None,
            },
        }
    )

    return list(page.hits)
