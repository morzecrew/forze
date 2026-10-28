"""Verifying an access token's session from cached revocation states.

A lookup reads the principal's epoch and the family's or the session's state from the states
store. A missing one is read from the database and written back, expiring ``ttl`` after the read
began; a revocation overwrites what lookups wrote, after its transaction commits. Every state
expires, so a revocation whose write never lands is enforced within ``ttl`` all the same.

An API key's state is keyed by its digest and is the only proof the key exists, so a live one
carries a MAC under a server key and an absolute not-after on the store's clock. Under the same
keys, a principal's epoch and live session states carry them too, so a store that can be
written cannot restore an older epoch or a ``live`` state for longer than ``ttl``.
"""

import asyncio
import hashlib
import hmac
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable, Sequence
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Literal, final
from uuid import UUID

import attrs

from forze.application.contracts.authn import (
    EPOCH_STATE_KEY,
    REVOKED_STATE,
    ApiKeyStateRead,
    ApiKeyStatesPort,
    PrincipalEpochPort,
    RevocationStatesPort,
    current_authn_admission,
    record_authn_admission,
)
from forze.application.contracts.deps import backend_of
from forze.application.contracts.document import DocumentQueryPort
from forze.application.contracts.transaction import TransactionallyEnlistable
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import StrKey, utcnow
from forze_identity._secure_spec import forbid_cache_and_history
from forze_identity._transaction import enlisted

from .._logger import logger
from ..domain.models.account import ReadApiKeyAccount
from ..domain.models.session import ReadSession
from .constants import MIN_SECRET_BYTES

# ----------------------- #

SessionBinding = Literal["session", "family"]
"""What a token's state is read under: its own session, or its session's family."""

RevocationEvent = Literal["hit", "miss", "fallback", "write_failure", "clock_skew"]
"""What the fast path counts: a verification answered from the states store alone (``hit``), one
that read the database for what the store lacked (``miss``), one whose store read failed and
that read the database instead (``fallback``), a revocation's write-through that failed every
attempt (``write_failure``), and a signed state the store's clock still accepted but the
application's clock put more than ``ttl`` past its not-after (``clock_skew``, a miss too)."""

RevocationCredential = Literal["session", "api_key"]
"""The credential a count is for."""

_LIVE = "live|"
_NO_TENANT = "-"

_ABSENT = "-"
"""An API-key state's actor or expiry when the key has none."""

_API_KEY_MAC_LABEL: Final = b"forze.authn.api-key-state.v1"
"""Starts every MAC input, so a key state's MAC verifies as nothing else the server key signs."""

_PRINCIPAL_STATE_MAC_LABEL: Final = b"forze.authn.principal-state.v1"
"""Starts the MAC input of a principal's epoch and session states, apart from a key state's."""

_API_KEY_MAC_KEY_LABEL: Final = b"forze.authn.api-key-state-mac-key.v1"
"""Derives each state MAC key from its configured secret, so a secret reused elsewhere (the API
key pepper above all, whose digests travel to Redis as key names) signs nothing a state takes."""

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MS = timedelta(milliseconds=1)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class RevocationStatesCounters:
    """Cumulative fast-path counts, by event and credential, for metrics export."""

    _counts: Counter[tuple[RevocationEvent, RevocationCredential]] = attrs.field(
        factory=Counter,
        init=False,
    )

    # ....................... #

    def add(self, event: RevocationEvent, credential: RevocationCredential) -> None:
        """Count one *event* for *credential*."""

        self._counts[event, credential] += 1

    def get(self, event: RevocationEvent, credential: RevocationCredential) -> int:
        """How many *event* for *credential* so far."""

        return self._counts[event, credential]


# ....................... #


def _mac(secret: bytes, message: bytes) -> str:
    key = hmac.new(secret, _API_KEY_MAC_KEY_LABEL, hashlib.sha256).digest()

    return hmac.new(key, message, hashlib.sha256).hexdigest()


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class StateMacKeys:
    """Server keys that authenticate cached revocation states, with HMAC-SHA256.

    Each deployment needs keys of its own, shared with no other deployment and no other secret:
    a state is bound to its store's namespace, but two deployments with one namespace name and
    one key accept each other's states. Each key is used through a key derived from it.

    A state is signed under :attr:`active` and accepted under it or any of :attr:`previous`. To
    rotate, make the new key active and keep the old one in :attr:`previous` for at least the
    states' ``ttl``, the longest a state signed under it lives; then drop it. A state signed under
    a key that is no longer configured is read from the database, never admitted.
    """

    active: bytes = attrs.field(repr=False)
    """Signs every state written from now on. At least :data:`MIN_SECRET_BYTES`."""

    previous: tuple[bytes, ...] = attrs.field(factory=tuple, converter=tuple, repr=False)
    """Accepted on verify only, until the states signed under them have expired."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        keys = (self.active, *self.previous)

        if not all(isinstance(key, bytes) for key in keys):
            raise exc.configuration("Revocation state MAC keys must be bytes")

        if any(len(key) < MIN_SECRET_BYTES for key in keys):
            raise exc.configuration(
                f"Revocation state MAC keys must be at least {MIN_SECRET_BYTES} bytes",
            )

    # ....................... #

    def sign(self, message: bytes) -> str:
        """The MAC of *message* under the active key, in hex."""

        return _mac(self.active, message)

    def verify(self, message: bytes, mac: str) -> bool:
        """Whether *mac* is the MAC of *message* under any configured key, in constant time."""

        if not mac.isascii():
            return False

        presented = mac.encode("ascii")
        matched = False

        # Every key is tried, so the time taken does not tell which one matched.
        for key in (self.active, *self.previous):
            matched |= hmac.compare_digest(_mac(key, message).encode("ascii"), presented)

        return matched


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AdmittedSession:
    """The session whose access token this request authenticated with."""

    principal_id: UUID
    session_id: UUID
    family_id: UUID | None
    epoch: int | None


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AdmittedApiKey:
    """The API key this request authenticated with, admitted while API-key states are on."""

    principal_id: UUID
    digest: str


@final
@attrs.define(slots=True, frozen=True)
class _Fresh:
    """A token admitted on its principal's current epoch, by a verification in *task*."""

    principal_id: UUID
    task: "asyncio.Task[Any] | None"


_FRESH: ContextVar[_Fresh | None] = ContextVar("forze_identity_fresh_admission", default=None)


def remember_admitted_session(admitted: AdmittedSession | None, *, fresh: bool = False) -> None:
    """Record the token-backed session this request authenticated with, or forget it.

    A credential verified later in the request replaces it: a password forgets it, and an API
    key replaces it with itself or forgets it, so an issue that follows re-checks only the
    credential the request authenticated with last. A transport that runs a connection's
    commands in other tasks carries the record to them.

    :param fresh: The verification compared the token's epoch with the principal's, so the
        eligibility call that follows it in this task may skip its read (see
        :func:`admitted_just_now`). Never carried to another task.
    """

    record_authn_admission(admitted)
    _FRESH.set(
        _Fresh(admitted.principal_id, asyncio.current_task())
        if fresh and admitted is not None
        else None
    )


def admitted_just_now(principal_id: UUID) -> bool:
    """Whether this task's last verification admitted a token of *principal_id* on its epoch.

    One answer per verification: the call consumes it, whatever it asks, so only the
    eligibility check that follows the verification can rely on it, never one after it.
    """

    fresh = _FRESH.get()

    if fresh is None:
        return False

    _FRESH.set(None)

    return (
        fresh.principal_id == principal_id
        and fresh.task is not None
        and fresh.task is asyncio.current_task()
    )


def remember_admitted_api_key(admitted: AdmittedApiKey | None, *, fresh: bool = False) -> None:
    """Record the API key this request authenticated with, or forget the credential before it.

    Carried to a connection's other tasks like a session's record.

    :param fresh: The key's state recorded its principal's current epoch, so the eligibility
        call that follows it in this task may skip its read, as after a token (see
        :func:`admitted_just_now`). Never carried to another task.
    """

    record_authn_admission(admitted)
    _FRESH.set(
        _Fresh(admitted.principal_id, asyncio.current_task())
        if fresh and admitted is not None
        else None
    )


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

    key_mac: StateMacKeys | None = None
    """When set, every live cached state carries a MAC under these keys and a not-after: API
    keys are verified from cached states too, and every revocation or deactivation of a key
    writes its state through; a principal's epoch and session states are signed, so the store
    is no longer trusted with them. The eligibility skip requires it. ``None`` leaves API keys
    on their row read and trusts the store with sessions."""

    counters: RevocationStatesCounters = attrs.field(
        factory=RevocationStatesCounters,
        init=False,
        eq=False,
        repr=False,
    )
    """Cumulative hit, miss, fallback and write-through failure counts, for metrics export."""

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


async def _bounded[T](config: RevocationStates, call: Awaitable[T]) -> T:
    async with asyncio.timeout(config.timeout.total_seconds()):
        return await call


def _ttl_ms(config: RevocationStates) -> int:
    return config.ttl // timedelta(milliseconds=1)


def _to_ms(moment: datetime) -> int:
    return (moment - _EPOCH) // _MS


def _mac_input(label: bytes, *fields: str) -> bytes:
    """*label* and *fields*, each length-prefixed so no two inputs read alike."""

    parts = (label, *(field.encode() for field in fields))

    return b"".join(len(part).to_bytes(4, "big") + part for part in parts)


def _expired(
    config: RevocationStates,
    credential: RevocationCredential,
    not_after_ms: int,
    *,
    now_ms: int,
) -> bool:
    """Whether a signed state read at the store's *now_ms* is past its not-after.

    The store's clock decides; the application's only caps a store that lies about it. Ahead of
    the store's by two ttl or more, it refuses every state: counted, so the cause shows.
    """

    if now_ms >= not_after_ms:
        return True

    if _to_ms(utcnow()) - config.ttl // _MS >= not_after_ms:
        config.counters.add("clock_skew", credential)
        return True

    return False


def _principal_message(config: RevocationStates, principal_id: UUID, key: str, body: str) -> bytes:
    """What a principal state's MAC covers: the label, the states route, the principal, the
    state key and the body."""

    return _mac_input(_PRINCIPAL_STATE_MAC_LABEL, config.route, str(principal_id), key, body)


def _seal(
    config: RevocationStates,
    principal_id: UUID,
    key: str,
    value: str,
    *,
    not_after_ms: int,
) -> str:
    """*value* as cached under *principal_id*'s *key*: with ``key_mac``, followed by its
    not-after and a MAC over both. ``revoked`` carries none: forging it can refuse, never admit."""

    if config.key_mac is None or value == REVOKED_STATE:
        return value

    body = f"{value}|{not_after_ms}"
    mac = config.key_mac.sign(_principal_message(config, principal_id, key, body))

    return f"{body}|{mac}"


def _unseal(
    config: RevocationStates,
    principal_id: UUID,
    key: str,
    raw: str | None,
    *,
    now_ms: int,
) -> str | None:
    """The value cached under *principal_id*'s *key*, or ``None`` when it proves nothing.

    With ``key_mac``, a value other than ``revoked`` counts only under a MAC that verifies and
    before its not-after: one unsigned, copied from another principal or key, or replayed past
    its not-after is a miss.
    """

    if raw is None or config.key_mac is None or raw == REVOKED_STATE:
        return raw

    body, _, mac = raw.rpartition("|")

    # Nothing in the state is read before its MAC verifies.
    if not body or not config.key_mac.verify(
        _principal_message(config, principal_id, key, body), mac
    ):
        return None

    # Signed, so written by _seal: a value, then an integer not-after.
    value, _, not_after = body.rpartition("|")

    return None if _expired(config, "session", int(not_after), now_ms=now_ms) else value


def _require_transaction(in_transaction: Callable[[], bool], *stores: object) -> None:
    """Refuse outside a transaction, or when one of *stores* would not write in it.

    A scope on another route, or on another database's client, is open but holds none of
    *stores*' writes: each would commit on its own, and a refusal after the first of them
    would leave it committed without the rest.
    """

    if not in_transaction():
        raise exc.configuration(
            "With revocation states configured, sessions are issued and revoked, and policy "
            "principals deactivated, only inside a transaction: an issue holds the principal's "
            "epoch under a lock, and a revocation raises it or writes its states through once "
            "it commits. Keep tx_route set on the authn and tenancy registries, or open a "
            "transaction around the call.",
            code="authn_revocation_requires_transaction",
        )

    for store in stores:
        backend = backend_of(store)

        if isinstance(backend, TransactionallyEnlistable) and not (
            backend.is_transactionally_enlisted()
        ):
            raise exc.configuration(
                f"The open transaction does not hold the writes of {type(backend).__name__}: "
                "with revocation states configured, a principal's epoch is raised in the "
                "transaction of the write it guards. Open the transaction on the route of "
                "the database that holds the epochs and the policy principals.",
                code="authn_revocation_requires_transaction",
            )


async def _write_through(
    config: RevocationStates,
    credential: RevocationCredential,
    principal_id: UUID,
    keys: list[str],
    write: Callable[[], Awaitable[None]],
) -> None:
    error: Exception | None = None

    for _ in range(config.write_attempts):
        try:
            await write()
            return

        except Exception as failure:
            error = failure

    # The revocation is committed; until these states expire, a stale one admits.
    config.counters.add("write_failure", credential)
    logger.error(
        "Revocation states write-through failed for principal %s, keys %s: %r",
        principal_id,
        keys,
        error,
    )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class EpochRaise:
    """Raise a principal's epoch in the caller's transaction, and write it through after commit.

    Every token issued before the raise is refused from then on; the eligibility read may be
    skipped for a token on the current epoch only because every deactivation raises it too.
    """

    config: RevocationStates
    """The one configuration the verifier and every writer read."""

    states: RevocationStatesPort
    """Cached states."""

    epochs: PrincipalEpochPort
    """Principal epochs, in the database."""

    in_transaction: Callable[[], bool]
    """Whether a transaction is open."""

    defer: Callable[[Callable[[], Awaitable[None]]], Awaitable[None]]
    """Runs a callback once the current transaction commits."""

    principals: DocumentQueryPort[Any] | None = None
    """The policy principals, on Postgres: when the open transaction holds their reads, the
    raise first reads the principal's row ``for_update``, as an issue does before it holds the
    epoch, so a deactivation of that row later in the transaction cannot deadlock with an issue.
    ``None`` where the raise follows a write that already took the row, where an issue locks no
    principal, and on a backend whose locking read excludes nothing (Mongo, the mock)."""

    # ....................... #

    def require_transaction(self, *stores: object) -> None:
        """Refuse to raise outside a transaction, or in one that holds neither the epoch's
        writes nor those of *stores* (the write the raise guards).

        :raises CoreException: ``configuration`` (``authn_revocation_requires_transaction``).
        """

        _require_transaction(self.in_transaction, self.epochs, *stores)

    async def __call__(self, principal_id: UUID) -> None:
        """Raise *principal_id*'s epoch, refusing every token issued before; write it after commit.

        :raises CoreException: ``configuration`` outside a transaction, or above read committed.
        """

        self.require_transaction()

        if self.principals is not None and enlisted(self.principals):
            # A raise then a deactivation in one transaction would otherwise take the epoch
            # before the principal, the reverse of an issue, and deadlock with it.
            await self.principals.find(filters={"$values": {"id": principal_id}}, for_update=True)

        await self.epochs.advance(principal_id)

        async def _write() -> None:
            # The epoch is read again at write time: a write delayed past a newer raise then
            # caches the newer epoch, or one that expires within ttl of the newer commit.
            now_ms = await _bounded(self.config, self.states.now_ms(principal_id))
            epoch = await self.epochs.current(principal_id)
            expire_at_ms = now_ms + _ttl_ms(self.config)

            if self.config.key_mac is None:
                # Unsigned, a store that predates signed states takes it as before.
                write = self.states.raise_epoch(principal_id, epoch, expire_at_ms=expire_at_ms)

            else:
                state = _seal(
                    self.config,
                    principal_id,
                    EPOCH_STATE_KEY,
                    str(epoch),
                    not_after_ms=expire_at_ms,
                )
                write = self.states.raise_epoch(
                    principal_id,
                    epoch,
                    expire_at_ms=expire_at_ms,
                    state=state,
                )

            await _bounded(self.config, write)

        await self.defer(
            lambda: _write_through(self.config, "session", principal_id, [EPOCH_STATE_KEY], _write),
        )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SessionRevocation:
    """The fast path's reads and writes for one request, shared by the verifier and the writers.

    Issuing holds the epoch under a share lock; a revocation of every session of a principal
    raises it, after locking the principal's row where an issue locks it; a revocation of
    families or sessions, and a rotation, write ``revoked`` for them after commit. Each of those
    refuses to run outside a transaction.
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

    principals: DocumentQueryPort[Any] | None = None
    """The policy principals each epoch raise locks first (:attr:`EpochRaise.principals`)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        forbid_cache_and_history(self.sessions.spec, label="Session")

    # ....................... #

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
    ) -> int:
        """Admit the token, or refuse it with the codes the session row would give.

        :param principal_id: The token's subject.
        :param session_id: The token's session.
        :param family_id: The token's family.
        :param pep: The epoch the token carries.
        :param tenant: The token's tenant claim.
        :returns: The principal's epoch the token was judged against.
        :raises CoreException: ``session_revoked``, ``session_subject_mismatch`` or
            ``session_tenant_mismatch``.
        """

        key = self._state_key(session_id, family_id)

        try:
            read = await _bounded(self.config, self.states.read(principal_id, key))

        except Exception as error:
            # Down, slow, or behind an open breaker: the same checks from the database.
            self.config.counters.add("fallback", "session")
            logger.warning(
                "Revocation states read failed, verifying the session from the database: %r",
                error,
            )
            epoch = await self.epochs.current(principal_id)
            state = await self._state_truth(
                principal_id, session_id, family_id, by_family=self._by_family
            )
            _judge(pep=pep, epoch=epoch, state=state, tenant=tenant)
            return epoch

        cached_epoch = _epoch(
            _unseal(self.config, principal_id, EPOCH_STATE_KEY, read.epoch, now_ms=read.now_ms),
        )
        cached_state = _state(
            _unseal(self.config, principal_id, key, read.state, now_ms=read.now_ms),
        )

        # Both move one way, so a cached revocation or a raised epoch refuses without a read.
        if (cached_epoch is not None and pep < cached_epoch) or cached_state == REVOKED_STATE:
            self.config.counters.add("hit", "session")
            raise _session_revoked()

        # Ahead of the cached epoch: a lookup that raced a raise cached the older value.
        epoch_cached = cached_epoch == pep
        hit = epoch_cached and cached_state is not None
        self.config.counters.add("hit" if hit else "miss", "session")

        missing: dict[str, str] = {}

        if epoch_cached:
            epoch = pep

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
            await self._fill(principal_id, missing, expire_at_ms=read.now_ms + _ttl_ms(self.config))

        _judge(pep=pep, epoch=epoch, state=state, tenant=tenant)

        return epoch

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
        # Signed to expire with the store's copy, so a replayed one outlives it by nothing.
        sealed = {
            key: _seal(self.config, principal_id, key, value, not_after_ms=expire_at_ms)
            for key, value in states.items()
        }

        try:
            await _bounded(
                self.config,
                self.states.fill(principal_id, sealed, expire_at_ms=expire_at_ms),
            )

        except Exception as error:
            logger.warning("Revocation states fill failed: %r", error)

    # ....................... #

    async def recheck_caller(self, principal_id: UUID, *, epoch: int | None = None) -> None:
        """Refuse a credential minted for *principal_id* when this request's own token is no longer live.

        A credential minted from a token outlives the token, so it must not be minted from one
        admitted inside the window, whoever it is minted for: the caller's credential is what
        must still be live. When this request (or the connection it runs on) authenticated with
        an access token, that token's session (or family, by the binding) is read again from
        the database, and the token's epoch compared with its principal's. *epoch* is the one
        the issue holds for *principal_id*, used when that is the caller; otherwise the caller's
        epoch is read here. Inside a transaction both are read under locks, the epoch first as
        every revocation takes it, so neither comes from a snapshot older than a revocation that
        has since committed; outside one, each read is a fresh snapshot of its own.

        :param principal_id: The principal the credential is minted for.
        :param epoch: The epoch the issue holds for *principal_id*, if it holds one.
        :raises CoreException: ``session_revoked``.
        """

        admitted = current_authn_admission()

        if not isinstance(admitted, AdmittedSession):
            return

        caller = admitted.principal_id
        lock = self.in_transaction()

        # The epoch before any session row: a principal-wide revocation raises the epoch and
        # then stamps the sessions, and the reverse order would deadlock with it. A token from
        # before the fast path carries no epoch to compare, and its session row is the guard.
        if admitted.epoch is not None and (epoch is None or principal_id != caller):
            epoch = await (self.epochs.hold if lock else self.epochs.current)(caller)

        state = await self._state_truth(
            caller,
            admitted.session_id,
            admitted.family_id,
            by_family=self._by_family,
            lock=lock,
        )

        if state == REVOKED_STATE or (
            admitted.epoch is not None and epoch is not None and admitted.epoch < epoch
        ):
            raise _session_revoked()

    def require_transaction(self, *stores: object) -> None:
        """Refuse to issue or revoke outside a transaction, or in one that does not hold the
        epoch's writes, or those of *stores* (the other ports the operation writes through).

        :raises CoreException: ``configuration`` (``authn_revocation_requires_transaction``).
        """

        _require_transaction(self.in_transaction, self.epochs, *stores)

    async def hold_epoch(self, principal_id: UUID) -> int:
        """Read the principal's epoch for a session being issued, holding it until commit."""

        self.require_transaction()

        return await self.epochs.hold(principal_id)

    async def raise_epoch(self, principal_id: UUID) -> None:
        """Raise the principal's epoch, refusing every token issued before; write it after commit."""

        await EpochRaise(
            config=self.config,
            states=self.states,
            epochs=self.epochs,
            in_transaction=self.in_transaction,
            defer=self.defer,
            principals=self.principals,
        )(principal_id)

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
            await _bounded(
                self.config,
                self.states.revoke(principal_id, keys, ttl=self.config.ttl),
            )

        await self.defer(lambda: _write_through(self.config, "session", principal_id, keys, _write))


# ....................... #


def _api_key_message(route: str, store_key: str, digest: str, body: str) -> bytes:
    """What an API-key state's MAC covers: the label, the states route, the store's full key for
    the state, the digest and the body."""

    return _mac_input(_API_KEY_MAC_LABEL, route, store_key, f"k:{digest}", body)


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class CachedApiKey:
    """An API key as its cached state proves it: active, under the digest it was found by."""

    principal_id: UUID
    actor_principal_id: UUID | None
    expires_at: datetime | None
    key_hash: str

    epoch: int | None = None
    """The principal's epoch the state records, read before the principal was found active;
    ``None`` in a state written without the eligibility skip, or by an older version."""

    @property
    def is_active(self) -> bool:
        """Always ``True``: a revoked key's state names no principal."""

        return True


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ApiKeyEligibilitySkip:
    """What lets a key admitted from its state skip the eligibility read, as a token does.

    On a miss, the key's state records the principal's epoch, read before the principal is found
    active, so an epoch that has not moved since implies nothing deactivated the principal: every
    deactivation raises it. Set only where the token skip applies.
    """

    states: RevocationStatesPort
    """Cached states, through their principal calls: the principal's cached epoch."""

    epochs: PrincipalEpochPort
    """Principal epochs, in the database."""

    principals: DocumentQueryPort[Any]
    """The policy principals, read on a miss after the epoch."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ApiKeyRevocation:
    """The API-key fast path's reads and writes, shared by the key verifier and the key writers.

    A key's state is keyed by its digest and is the only proof the key exists: the verifier takes
    the principal, the actor and the expiry from it. So a live state carries a MAC under
    :attr:`RevocationStates.key_mac` over its digest and every field, and a not-after: the
    earlier of the store's time when the lookup read plus ``ttl``, and the key's own expiry. A
    state whose MAC fails, or that is past its not-after on the store's clock from the same read,
    is a miss. ``revoked`` refuses without a read. An issue from a request a key authenticated
    reads the key's row again first (:meth:`recheck_caller`). With :attr:`skip`, a state also
    records its principal's epoch, and a key whose recorded epoch is the principal's cached one
    skips the eligibility read that follows (:meth:`fresh`).
    """

    config: RevocationStates
    """The one configuration the verifier and every writer read; :attr:`~RevocationStates.key_mac`
    must be set."""

    states: ApiKeyStatesPort
    """Cached states, through their API-key calls."""

    accounts: DocumentQueryPort[ReadApiKeyAccount]
    """API key rows, read on a miss and by :meth:`recheck_caller`."""

    in_transaction: Callable[[], bool]
    """Whether a transaction is open."""

    defer: Callable[[Callable[[], Awaitable[None]]], Awaitable[None]]
    """Runs a callback once the current transaction commits, or at once outside one."""

    skip: ApiKeyEligibilitySkip | None = None
    """Set where the eligibility read may be skipped; ``None`` leaves every key to that read."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        # Refused at construction rather than on the first key presented.
        _ = self._mac
        forbid_cache_and_history(self.accounts.spec, label="API key account")

    @property
    def _mac(self) -> StateMacKeys:
        if self.config.key_mac is None:
            raise exc.configuration("API-key revocation states need RevocationStates.key_mac")

        return self.config.key_mac

    # ....................... #

    async def _row(self, digest: str, *, lock: bool = False) -> ReadApiKeyAccount | None:
        return await self.accounts.find(filters={"$values": {"key_hash": digest}}, for_update=lock)

    async def find(self, digest: str) -> CachedApiKey | ReadApiKeyAccount | None:
        """The key under *digest*: from its cached state, or from its row on a miss.

        ``None`` for a revoked or unknown key. On a miss, outside a transaction, the row read is
        written back with ``SET NX``, so it never replaces a present state.

        :param digest: The presented key's digest.
        """

        try:
            read = await _bounded(self.config, self.states.read_api_key(digest))

        except Exception as error:
            self.config.counters.add("fallback", "api_key")
            logger.warning(
                "Revocation states read failed, verifying the API key from the database: %r",
                error,
            )
            return await self._row(digest)

        if read.state == REVOKED_STATE:
            self.config.counters.add("hit", "api_key")
            return None

        if read.state is not None:
            cached = self._live(digest, read.state, read=read)

            if cached is not None:
                self.config.counters.add("hit", "api_key")
                return cached

        self.config.counters.add("miss", "api_key")
        account = await self._row(digest)

        # Inside a transaction the read may predate a revocation, or be the revocation's. A
        # state present but unproven is never replaced, so there is nothing to write.
        if read.state is None and not self.in_transaction():
            await self._fill(digest, account, read=read)

        return account

    def _message(self, read: ApiKeyStateRead, digest: str, body: str) -> bytes:
        return _api_key_message(self.config.route, read.key, digest, body)

    def _live(self, digest: str, raw: str, *, read: ApiKeyStateRead) -> CachedApiKey | None:
        """The key a cached state proves, or ``None`` when it proves nothing."""

        body, _, mac = raw.rpartition("|")

        # Nothing in the state is read before its MAC verifies.
        if not body or not self._mac.verify(self._message(read, digest, body), mac):
            return None

        try:
            # The principal's epoch sits before the not-after only in a state written to skip.
            live, principal, actor, expires, *epoch, not_after = body.split("|")

            if (
                live != "live"
                or len(epoch) > 1
                or _expired(self.config, "api_key", int(not_after), now_ms=read.now_ms)
            ):
                return None

            return CachedApiKey(
                principal_id=UUID(principal),
                actor_principal_id=None if actor == _ABSENT else UUID(actor),
                expires_at=None if expires == _ABSENT else _EPOCH + int(expires) * _MS,
                key_hash=digest,
                epoch=int(epoch[0]) if epoch else None,
            )

        except ValueError:
            return None

    async def fresh(self, key: CachedApiKey) -> bool:
        """Whether *key*'s state recorded its principal's current epoch: then nothing has
        deactivated the principal since the state was written, and the eligibility read that
        follows may be skipped.

        The epoch is the principal's cached one, signed as a token reads it. When it is missing,
        the database's is compared instead and written back: as sound, since a deactivation
        raises it in its own transaction, and one read where the eligibility check would make
        one. Inside a transaction it never skips: the transaction may have raised the epoch
        itself, which reaches the cache only after commit. Anything else reads eligibility.
        """

        if self.skip is None or key.epoch is None or self.in_transaction():
            return False

        principal_id = key.principal_id

        try:
            read = await _bounded(self.config, self.skip.states.read(principal_id, EPOCH_STATE_KEY))

        except Exception as error:
            self.config.counters.add("fallback", "api_key")
            logger.warning("Revocation states read failed, reading eligibility: %r", error)
            return False

        cached = _epoch(
            _unseal(self.config, principal_id, EPOCH_STATE_KEY, read.epoch, now_ms=read.now_ms),
        )

        if cached is not None:
            return cached == key.epoch

        try:
            epoch = await self.skip.epochs.current(principal_id)

        except Exception as error:
            self.config.counters.add("fallback", "api_key")
            logger.warning("Principal epoch read failed, reading eligibility: %r", error)
            return False

        await self._fill_epoch(principal_id, epoch, expire_at_ms=read.now_ms + _ttl_ms(self.config))

        return epoch == key.epoch

    async def _fill_epoch(self, principal_id: UUID, epoch: int, *, expire_at_ms: int) -> None:
        """Write *principal_id*'s epoch, read from the database, back where it is missing."""

        if self.skip is None:
            return

        state = _seal(
            self.config, principal_id, EPOCH_STATE_KEY, str(epoch), not_after_ms=expire_at_ms
        )

        try:
            await _bounded(
                self.config,
                self.skip.states.fill(
                    principal_id, {EPOCH_STATE_KEY: state}, expire_at_ms=expire_at_ms
                ),
            )

        except Exception as error:
            logger.warning("Revocation states fill failed: %r", error)

    async def _eligible_epoch(self, principal_id: UUID) -> int | None:
        """The principal's epoch, when the principal is active after it was read; else ``None``.

        In that order: a deactivation that committed before the epoch read is seen by the
        principal's read after it, and one after it has raised the epoch the state records.
        """

        if self.skip is None:
            return None

        epoch = await self.skip.epochs.current(principal_id)
        principal = await self.skip.principals.find(filters={"$values": {"id": principal_id}})

        return epoch if principal is not None and principal.is_active else None

    async def _fill(
        self,
        digest: str,
        account: ReadApiKeyAccount | None,
        *,
        read: ApiKeyStateRead,
    ) -> None:
        # An unknown digest is not cached: anyone can present one.
        if account is None:
            return

        now_ms = read.now_ms
        expire_at_ms = now_ms + self.config.ttl // _MS
        epoch: int | None = None

        if not account.is_active:
            state = REVOKED_STATE

        else:
            expires_ms = None

            if account.expires_at is not None:
                expires_ms = _to_ms(account.expires_at)
                expire_at_ms = min(expire_at_ms, expires_ms)

            # An expired key is refused from its row; nothing to cache.
            if expire_at_ms <= now_ms:
                return

            actor = account.actor_principal_id
            epoch = await self._eligible_epoch(account.principal_id)
            body = "|".join(
                (
                    "live",
                    str(account.principal_id),
                    _ABSENT if actor is None else str(actor),
                    _ABSENT if expires_ms is None else str(expires_ms),
                    # Only when there is one, so a state without it reads as an older version's.
                    *(() if epoch is None else (str(epoch),)),
                    str(expire_at_ms),
                ),
            )
            state = f"{body}|{self._mac.sign(self._message(read, digest, body))}"

        try:
            # Expires in the store at its not-after, so it never outlives what its MAC names.
            await _bounded(
                self.config,
                self.states.fill_api_key(digest, state, expire_at_ms=expire_at_ms),
            )

        except Exception as error:
            logger.warning("Revocation states fill failed: %r", error)

        if account.is_active and epoch is not None:
            # So the next verification compares the epoch from the cache, not the database.
            await self._fill_epoch(account.principal_id, epoch, expire_at_ms=expire_at_ms)

    # ....................... #

    async def recheck_caller(self) -> None:
        """Refuse an issue this request's own API key no longer allows, whoever it is for.

        A credential minted from a key outlives the key, so it must not be minted from one
        admitted inside the window, for its own principal or anyone else. When this request
        authenticated with an API key, the key's row is read again, under a lock inside a
        transaction, so the read is no older than a revocation that has since committed.

        :raises CoreException: ``api_key_revoked``.
        """

        admitted = current_authn_admission()

        if not isinstance(admitted, AdmittedApiKey):
            return

        row = await self._row(admitted.digest, lock=self.in_transaction())

        if (
            row is None
            or not row.is_active
            or row.principal_id != admitted.principal_id
            or (row.expires_at is not None and row.expires_at <= utcnow())
        ):
            raise exc.authentication("API key revoked", code="api_key_revoked")

    # ....................... #

    async def revoke(self, accounts: Sequence[ReadApiKeyAccount]) -> None:
        """Write ``revoked`` for *accounts*' keys once the current transaction commits.

        Outside a transaction the write runs at once, after the deactivation it follows has
        committed. Retried ``write_attempts`` times; a write that fails them all is logged with
        the key ids and counted, and the revocation stands.
        """

        if not accounts:
            return

        digests = [account.key_hash for account in accounts]
        keys = [f"api_key:{account.id}" for account in accounts]

        async def _write() -> None:
            await _bounded(
                self.config,
                self.states.revoke_api_keys(digests, ttl=self.config.ttl),
            )

        await self.defer(
            lambda: _write_through(
                self.config,
                "api_key",
                accounts[0].principal_id,
                keys,
                _write,
            ),
        )
