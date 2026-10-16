"""Verifying an access token's session from cached revocation states.

A lookup reads the principal's epoch and the family's or the session's state from the states
store. A missing one is read from the database and written back, expiring ``ttl`` after the read
began; a revocation overwrites what lookups wrote, after its transaction commits. Every state
expires, so a revocation whose write never lands is enforced within ``ttl`` all the same.
"""

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from contextvars import ContextVar
from datetime import timedelta
from typing import Literal, final
from uuid import UUID

import attrs

from forze.application.contracts.authn import (
    EPOCH_STATE_KEY,
    REVOKED_STATE,
    PrincipalEpochPort,
    RevocationStatesPort,
)
from forze.application.contracts.document import DocumentQueryPort
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import StrKey
from forze_identity._secure_spec import forbid_cache_and_history

from .._logger import logger
from ..domain.models.session import ReadSession

# ----------------------- #

SessionBinding = Literal["session", "family"]
"""What a token's state is read under: its own session, or its session's family."""

_LIVE = "live|"
_NO_TENANT = "-"

# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AdmittedSession:
    """The session whose access token this request authenticated with."""

    principal_id: UUID
    session_id: UUID
    family_id: UUID | None
    epoch: int | None


_ADMITTED: ContextVar[AdmittedSession | None] = ContextVar(
    "forze_identity_admitted_session",
    default=None,
)


def remember_admitted_session(admitted: AdmittedSession | None) -> None:
    """Record the token-backed session this request authenticated with, or forget it.

    A credential verified later in the request replaces it: a password or an API key forgets it,
    so an issue that follows re-checks only the credential the request authenticated with last.
    """

    _ADMITTED.set(admitted)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class RevocationStates:
    """Verify access tokens from cached revocation states instead of the session row.

    Opt-in. Tokens then carry the session's family (``fam``) and the principal's epoch
    (``pep``). A verification reads the epoch and one state from the states store, and reads
    the database only for what it misses. Every revocation writes its states through after it
    commits. Configure every process that verifies or revokes: a revocation made where this is
    absent is enforced only once the states expire.
    """

    route: StrKey
    """Route of the revocation states port (``RevocationStatesDepKey``): Redis, or the mock."""

    ttl: timedelta
    """How long a cached state lives. A revoked credential can be admitted for at most this
    long after its revocation commits, and only when the revocation's write never lands."""

    write_attempts: int = 3
    """Attempts a revocation's write-through gets before it is logged as failed."""

    timeout: timedelta = timedelta(milliseconds=50)
    """Bound on each call to the states store. A slower call reads the database instead."""

    session_binding: SessionBinding = "session"
    """``"session"``: a token is refused once its own session is rotated, as without the fast
    path. ``"family"``: admitted while its session's family is live, so a refresh elsewhere
    leaves it valid until its ``exp``; a logout, a family revocation or reuse still refuse it."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.ttl < timedelta(milliseconds=1):
            raise exc.configuration("Revocation states ttl must be at least 1 millisecond")

        if self.write_attempts < 1:
            raise exc.configuration("Revocation states write_attempts must be at least 1")

        if self.timeout <= timedelta(0):
            raise exc.configuration("Revocation states timeout must be positive")

        if self.session_binding not in ("session", "family"):
            raise exc.configuration(
                f"Unknown session_binding {self.session_binding!r}: expected 'session' or "
                "'family'.",
            )


# ....................... #


def _session_revoked() -> CoreException:
    return exc.authentication("Session revoked", code="session_revoked")


def _epoch(raw: str | None) -> int | None:
    """A cached epoch, or ``None`` when absent or not a decimal integer."""

    if raw is None or not raw.isascii() or not raw.isdigit():
        return None

    return int(raw)


def _state(raw: str | None) -> str | None:
    """A cached state, or ``None`` when absent or of no known shape."""

    if raw is None or raw == REVOKED_STATE:
        return raw

    if not raw.startswith(_LIVE):
        return None

    tenant = raw.removeprefix(_LIVE)

    if tenant != _NO_TENANT:
        try:
            UUID(tenant)

        except ValueError:
            return None

    return raw


def _judge(*, pep: int, epoch: int, state: str, tenant: str | None) -> None:
    if pep < epoch or state == REVOKED_STATE:
        raise _session_revoked()

    held = state.removeprefix(_LIVE)

    if tenant is not None and held != _NO_TENANT and held != tenant:
        raise exc.authentication(
            "Session does not match token tenant",
            code="session_tenant_mismatch",
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SessionRevocation:
    """The fast path's reads and writes for one request, shared by the verifier and the writers.

    Issuing holds the epoch under a share lock; a revocation of every session of a principal
    raises it; a revocation of families or sessions, and a rotation, write ``revoked`` for them
    after commit. Each of those refuses to run outside a transaction.
    """

    config: RevocationStates
    """The one configuration the verifier and every writer read."""

    states: RevocationStatesPort
    """Cached states."""

    epochs: PrincipalEpochPort
    """Principal epochs, in the database."""

    sessions: DocumentQueryPort[ReadSession]
    """Session rows, read on a miss."""

    in_transaction: Callable[[], bool]
    """Whether a transaction is open."""

    defer: Callable[[Callable[[], Awaitable[None]]], Awaitable[None]]
    """Runs a callback once the current transaction commits."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        forbid_cache_and_history(self.sessions.spec, label="Session")

    # ....................... #

    async def _bounded[T](self, call: Awaitable[T]) -> T:
        async with asyncio.timeout(self.config.timeout.total_seconds()):
            return await call

    def _ttl_ms(self) -> int:
        return self.config.ttl // timedelta(milliseconds=1)

    @property
    def _by_family(self) -> bool:
        return self.config.session_binding == "family"

    def _state_key(self, session_id: UUID, family_id: UUID) -> str:
        if self._by_family:
            return f"f:{family_id}"

        return f"s:{session_id}"

    # ....................... #

    async def check(
        self,
        *,
        principal_id: UUID,
        session_id: UUID,
        family_id: UUID,
        pep: int,
        tenant: str | None,
    ) -> None:
        """Admit the token, or refuse it with the codes the session row would give.

        :param principal_id: The token's subject.
        :param session_id: The token's session.
        :param family_id: The token's family.
        :param pep: The epoch the token carries.
        :param tenant: The token's tenant claim.
        :raises CoreException: ``session_revoked``, ``session_subject_mismatch`` or
            ``session_tenant_mismatch``.
        """

        key = self._state_key(session_id, family_id)

        try:
            read = await self._bounded(self.states.read(principal_id, key))

        except Exception as error:
            # Down, slow, or behind an open breaker: the same checks from the database.
            logger.warning(
                "Revocation states read failed, verifying the session from the database: %r",
                error,
            )
            epoch = await self.epochs.current(principal_id)
            state = await self._state_truth(
                principal_id, session_id, family_id, by_family=self._by_family
            )
            _judge(pep=pep, epoch=epoch, state=state, tenant=tenant)
            return

        cached_epoch = _epoch(read.epoch)
        cached_state = _state(read.state)

        # Both move one way, so a cached revocation or a raised epoch refuses without a read.
        if (cached_epoch is not None and pep < cached_epoch) or cached_state == REVOKED_STATE:
            raise _session_revoked()

        missing: dict[str, str] = {}

        # Ahead of the cached epoch: a lookup that raced a raise cached the older value.
        if cached_epoch is not None and pep == cached_epoch:
            epoch = cached_epoch

        else:
            epoch = await self.epochs.current(principal_id)

            if cached_epoch is None:
                missing[EPOCH_STATE_KEY] = str(epoch)

        if cached_state is not None:
            state = cached_state

        else:
            state = await self._state_truth(
                principal_id, session_id, family_id, by_family=self._by_family
            )
            missing[key] = state

        if missing and not self.in_transaction():
            # Inside a transaction the read may predate a revocation, or be the revocation's.
            await self._fill(principal_id, missing, expire_at_ms=read.now_ms + self._ttl_ms())

        _judge(pep=pep, epoch=epoch, state=state, tenant=tenant)

    # ....................... #

    async def _state_truth(
        self,
        principal_id: UUID,
        session_id: UUID,
        family_id: UUID | None,
        *,
        by_family: bool,
        lock: bool = False,
    ) -> str:
        """The family's or the session's state, from the session rows.

        With *lock*, the row the state rests on (the session, or the family's live member) is
        read ``for_update``: the latest committed row at read committed, and a serialization
        failure at a stricter level when it changed after the transaction's snapshot.
        """

        row: ReadSession | None

        if by_family and family_id is not None:
            page = await self.sessions.find_many(
                filters={
                    "$values": {
                        "family_id": family_id,
                        "revoked_at": None,
                        "rotated_at": None,
                    },
                },
                pagination={"limit": 1},
            )
            row = page.hits[0] if page.hits else None

            if row is not None and lock:
                row = await self.sessions.find(
                    filters={"$values": {"id": row.id}},
                    for_update=True,
                )

        else:
            row = await self.sessions.find(
                filters={"$values": {"id": session_id}},
                for_update=lock,
            )

        if row is not None and (row.revoked_at is not None or row.rotated_at is not None):
            row = None

        if row is None:
            return REVOKED_STATE

        if row.principal_id != principal_id:
            raise exc.authentication(
                "Session does not match token subject",
                code="session_subject_mismatch",
            )

        tenant = str(row.tenant_id) if row.tenant_id is not None else _NO_TENANT

        return f"{_LIVE}{tenant}"

    async def _fill(self, principal_id: UUID, states: dict[str, str], *, expire_at_ms: int) -> None:
        try:
            await self._bounded(self.states.fill(principal_id, states, expire_at_ms=expire_at_ms))

        except Exception as error:
            logger.warning("Revocation states fill failed: %r", error)

    # ....................... #

    async def recheck_caller(self, principal_id: UUID, *, epoch: int | None = None) -> None:
        """Refuse an issue for *principal_id* that this request's own token no longer allows.

        A credential minted from a token outlives the token, so it must not be minted from one
        admitted inside the window. When this request authenticated with an access token of
        the same subject, its session (or family, by the binding) is read again from the
        database, and its epoch compared with *epoch*, the one the issue holds, or else a fresh
        one. Inside a transaction both are read under locks, so neither comes from a snapshot
        older than a revocation that has since committed; outside one, each read is a fresh
        snapshot of its own.

        :raises CoreException: ``session_revoked``.
        """

        admitted = _ADMITTED.get()

        if admitted is None or admitted.principal_id != principal_id:
            return

        lock = self.in_transaction()
        state = await self._state_truth(
            principal_id,
            admitted.session_id,
            admitted.family_id,
            by_family=self._by_family,
            lock=lock,
        )

        if epoch is None:
            epoch = await (self.epochs.hold if lock else self.epochs.current)(principal_id)

        if state == REVOKED_STATE or (admitted.epoch is not None and admitted.epoch < epoch):
            raise _session_revoked()

    def require_transaction(self) -> None:
        """Refuse to issue or revoke outside a transaction.

        :raises CoreException: ``configuration`` (``authn_revocation_requires_transaction``).
        """

        if not self.in_transaction():
            raise exc.configuration(
                "With revocation states configured, sessions are issued and revoked only inside "
                "a transaction: an issue holds the principal's epoch under a lock, and a "
                "revocation writes its states through once it commits. Keep tx_route set on "
                "the authn and tenancy registries, or open a transaction around the call.",
                code="authn_revocation_requires_transaction",
            )

    async def hold_epoch(self, principal_id: UUID) -> int:
        """Read the principal's epoch for a session being issued, holding it until commit."""

        self.require_transaction()

        return await self.epochs.hold(principal_id)

    async def raise_epoch(self, principal_id: UUID) -> None:
        """Raise the principal's epoch, refusing every token issued before; write it after commit."""

        self.require_transaction()
        await self.epochs.advance(principal_id)

        async def _write() -> None:
            # The epoch is read again at write time: a write delayed past a newer raise then
            # caches the newer epoch, or one that expires within ttl of the newer commit.
            now_ms = await self._bounded(self.states.now_ms(principal_id))
            epoch = await self.epochs.current(principal_id)
            await self._bounded(
                self.states.raise_epoch(
                    principal_id,
                    epoch,
                    expire_at_ms=now_ms + self._ttl_ms(),
                ),
            )

        await self.defer(lambda: self._write_through(principal_id, [EPOCH_STATE_KEY], _write))

    async def revoke(
        self,
        principal_id: UUID,
        *,
        sessions: Iterable[ReadSession] = (),
        families: Iterable[UUID] = (),
    ) -> None:
        """Write ``revoked`` after commit for *sessions*, their families, and *families*."""

        keys: list[str] = []

        for session in sessions:
            keys += [f"s:{session.id}", f"f:{session.family_id}"]

        keys += [f"f:{family_id}" for family_id in families]
        await self._revoke_keys(principal_id, list(dict.fromkeys(keys)))

    async def rotated(self, principal_id: UUID, session_id: UUID) -> None:
        """Write ``revoked`` after commit for a session a refresh replaced."""

        await self._revoke_keys(principal_id, [f"s:{session_id}"])

    async def _revoke_keys(self, principal_id: UUID, keys: list[str]) -> None:
        # Outside a transaction the write would run inline, before any revocation committed.
        self.require_transaction()

        async def _write() -> None:
            await self._bounded(
                self.states.revoke(principal_id, keys, ttl=self.config.ttl),
            )

        await self.defer(lambda: self._write_through(principal_id, keys, _write))

    async def _write_through(
        self,
        principal_id: UUID,
        keys: list[str],
        write: Callable[[], Awaitable[None]],
    ) -> None:
        error: Exception | None = None

        for _ in range(self.config.write_attempts):
            try:
                await write()
                return

            except Exception as failure:
                error = failure

        # The revocation is committed; until these states expire, a stale one admits.
        logger.error(
            "Revocation states write-through failed for principal %s, keys %s: %r",
            principal_id,
            keys,
            error,
        )
