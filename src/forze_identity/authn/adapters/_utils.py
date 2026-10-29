from collections.abc import Mapping
from typing import Any
from uuid import UUID

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
    KeyedUpdate,
)
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import utcnow
from forze_identity._transaction import enlisted

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
    *,
    lock: bool = False,
) -> ReadPasswordAccount | None:
    """The principal's password account.

    :param lock: Read it row-locked when the open transaction holds the store, as a password
        change, a reset and a deactivation do right before they write it: a concurrent one then
        waits and reads the account as this one left it, instead of failing on its revision. A
        store that cannot report its enlistment is read without the lock.
    """

    return await qry.find(
        filters={
            "$values": {
                "principal_id": principal_id,
            },
        },
        for_update=lock and enlisted(qry),
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

_API_KEY_LIST_LIMIT = 200
"""Cap on keys returned by a principal's key list (self-service management — a
principal accumulating more than this is pathological; the page is bounded, not
silently truncated mid-feature)."""


async def find_api_key_accounts_by_principal(
    qry: DocumentQueryPort[ReadApiKeyAccount],
    principal_id: UUID,
) -> list[ReadApiKeyAccount]:
    # Fetch one past the cap so an overflow is detected rather than silently truncated.
    page = await qry.find_many(
        filters={
            "$values": {
                "principal_id": principal_id,
            },
        },
        pagination={"limit": _API_KEY_LIST_LIMIT + 1},
    )

    if len(page.hits) > _API_KEY_LIST_LIMIT:
        raise exc.internal(
            f"Principal {principal_id} has more than {_API_KEY_LIST_LIMIT} API keys; "
            "refusing to return a silently-truncated list.",
        )

    return list(page.hits)


# ....................... #


_REVOKE_BATCH = 1_000
"""Sessions revoked per round trip."""

STALE_RETRIES = 3
"""Consecutive writes a concurrent write may make stale before a revocation or a deactivation
gives up."""


def detected_stale(error: CoreException) -> bool:
    """Whether *error* is a stale batch the store detected itself, its transaction usable.

    Stale shows up as a revision mismatch when the read for the write sees the new revision,
    and as a conditional update that matched no row when it does not. Either is raised by
    the store with no driver error behind it. A conflict the backend reports arrives
    translated from a driver error, and the backend has aborted the transaction with it.
    """

    if error.code != "revision_mismatch" and error.kind is not ExceptionKind.CONCURRENCY:
        return False

    seen: set[int] = set()
    current = error.__cause__ or error.__context__

    while current is not None and id(current) not in seen:
        if not isinstance(current, CoreException):
            return False

        seen.add(id(current))
        current = current.__cause__ or current.__context__

    return True


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
    *,
    collect: bool = False,
) -> list[ReadSession]:
    """Revoke (set ``revoked_at``) every live session matching the ``$values`` filter.

    Shared by cascaded principal deactivation (token lifecycle) and the
    password-change "log out everywhere" cascade. Only live sessions match: a revoked
    or rotated one is already refused. Nothing revokes a session when it expires, so a
    principal can hold any number of them, and they are revoked in batches until a batch
    comes back short; each batch's revocations drop those rows out of the filter.

    A concurrent refresh can rotate a session between the read and the write, which makes
    its revision stale and refuses the batch. The batch is then read again, so the rotated
    session drops out and the session that replaced it is revoked instead; a batch that
    goes stale more than a few times in a row raises. A conflict the backend itself reports
    (a deadlock, a Mongo write conflict) has aborted the transaction, so it propagates for
    the transaction's owner to retry in a fresh one.

    :param collect: Return the revoked sessions; otherwise the list is empty, so a revocation
        of many sessions holds none of them.
    :returns: The sessions it revoked, as they were read, when *collect*.
    """

    filters: Any = {
        "$values": {
            **values,
            "revoked_at": None,
            "rotated_at": None,
        },
    }

    stale = 0
    revoked: list[ReadSession] = []

    while True:
        sessions = await session_qry.find_many(
            filters=filters,
            pagination={"limit": _REVOKE_BATCH},
        )

        if not sessions.hits:
            return revoked

        upds = [
            KeyedUpdate(id=x.id, rev=x.rev, dto=UpdateSessionCmd(revoked_at=utcnow()))
            for x in sessions.hits
        ]

        try:
            await session_cmd.update_many(upds, return_new=False)

        except CoreException as error:
            if not detected_stale(error) or stale >= STALE_RETRIES:
                raise

            stale += 1
            continue

        stale = 0

        if collect:
            revoked += sessions.hits

        if len(sessions.hits) < _REVOKE_BATCH:
            return revoked


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
