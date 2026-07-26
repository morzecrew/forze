"""MongoDB rotation target — the backend-specific steps of a URI rotation.

The design mirrors the Postgres target, because the constraint is the same: MongoDB holds
one password per user, so rotating in place leaves a window where *new* authentications
with the old credential fail.

- **Dual-user alternation (the default).** Two users (``app_a``/``app_b``) with matching
  roles; the secret's URI names one. A rotation composes a URI naming the *idle* user with
  a fresh password, sets that password with ``updateUser``, verifies it with a real
  authenticated connection, and promotes. The previously-active user keeps its password
  through the whole propagation window and becomes the idle target of the next rotation, so
  no credential in flight is ever invalid.

- **Single-user mode (degraded, explicit opt-in).** ``updateUser`` on the URI's own user.
  Established connections survive (MongoDB authenticates at connection time), so with
  connect-time re-resolution the blast radius is retry noise — but it is degraded and must
  be acknowledged via ``single_user_degraded=True``.

The admin client must authenticate as a user holding ``userAdmin`` on the users' database
(``admin`` unless :attr:`user_database` says otherwise) and reach the same deployment the
rotated URI points at.

**The apply is bounded server-side.** ``updateUser`` accepts ``maxTimeMS``, and a command
that exceeds it is killed by the server with the write *not* applied — verified against a
live server rather than assumed, because a client-side timeout would not do: abandoning a
request does not stop a write already in progress. That bound plus the client-side wait
before a command reaches a server is what :attr:`apply_latency_bound` declares, and the
rotator refuses a reconfirmation window that does not strictly exceed it.
"""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from datetime import timedelta
from typing import final
from uuid import UUID

import attrs
from pymongo import AsyncMongoClient

from forze.application.contracts.secrets import (
    PendingCredential,
    RotationTargetPort,
    SecretsPort,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from ..kernel.client import MongoClientPort
from ..kernel.uri import mongo_uri_password, mongo_uri_username, with_mongo_credentials

# ----------------------- #

_ADMIN_DB = "admin"


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MongoRotationTarget(RotationTargetPort):
    """:class:`~forze.application.contracts.secrets.RotationTargetPort` for MongoDB URIs.

    The staged value is resolved through :attr:`secrets` at call time — steps carry
    ``{ref, version}`` only, never credential text. Secrets at the rotated ref must be
    ``mongodb://`` or ``mongodb+srv://`` connection strings.
    """

    secrets: SecretsPort
    """Store the pending value is resolved from (the same store the rotator stages to)."""

    client: MongoClientPort
    """Admin connection used for ``updateUser`` (needs ``userAdmin`` on the users' database)."""

    user_pair: tuple[str, str] | None = None
    """The alternating user pair for dual-user rotation. The URI's current user must be one
    of the two; the rotation targets the other."""

    single_user_degraded: bool = False
    """Explicit acknowledgment of single-user mode (see the module docstring for exactly
    what degrades). Mutually exclusive with :attr:`user_pair`."""

    user_database: str = _ADMIN_DB
    """Database the managed users live in — where ``updateUser`` is run and what a rotated
    URI authenticates against."""

    verify_timeout: timedelta = timedelta(seconds=10)
    """Server-selection and connect timeout for the verification connection."""

    apply_max_time: timedelta = timedelta(seconds=30)
    """Server-side ``maxTimeMS`` for the ``updateUser`` — always enforced.

    This is what makes a stale in-flight apply *boundedly* stale: a command the server has
    not completed within this bound is killed and its write never lands. An unbounded apply
    would defeat the delayed-reconfirmation physics entirely, so there is deliberately no
    opt-out; a deployment bounding commands elsewhere sets this to match."""

    dispatch_allowance: timedelta = timedelta(seconds=30)
    """Client-side latency a stale apply can spend *before* the server clock starts.

    ``maxTimeMS`` only ticks once the command reaches a server; before that it waits on
    server selection and connection establishment. Set this to at least that wait so
    :attr:`apply_latency_bound` covers the command's whole possible lifetime — it is
    validated against the client's own configured timeouts."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.user_pair is not None and self.single_user_degraded:
            raise exc.configuration(
                "Choose dual-user rotation (user_pair) or single-user mode "
                "(single_user_degraded=True), not both.",
            )

        if self.user_pair is None and not self.single_user_degraded:
            raise exc.configuration(
                "MongoDB rotation needs a user_pair for dual-user alternation; single-user "
                "updateUser is degraded and requires the explicit "
                "single_user_degraded=True opt-in.",
            )

        if self.user_pair is not None and self.user_pair[0] == self.user_pair[1]:
            raise exc.configuration("Dual-user rotation needs two distinct users.")

        if self.verify_timeout.total_seconds() <= 0:
            raise exc.configuration("Verify timeout must be positive")

        if self.apply_max_time.total_seconds() <= 0:
            # No unbounded escape hatch: the delayed reconfirmation's coverage is only as
            # real as this bound.
            raise exc.configuration(
                "Apply max time must be positive; an unbounded updateUser defeats the "
                "delayed-reconfirmation bound.",
            )

        if self.dispatch_allowance.total_seconds() < 0:
            raise exc.configuration("Dispatch allowance must not be negative")

        self._validate_dispatch_allowance()

    # ....................... #

    def _validate_dispatch_allowance(self) -> None:
        """Tie the allowance to the client's CONFIGURED dispatch wait, not an independent
        estimate: an allowance below the real wait understates ``apply_latency_bound`` and
        reopens the late-apply window. Checked at construction and again at apply time (the
        authoritative moment — a client initialized after this target was built carries its
        final value by then)."""

        configured = getattr(self.client, "command_dispatch_bound", None)

        if isinstance(configured, timedelta) and configured > self.dispatch_allowance:
            raise exc.configuration(
                f"dispatch_allowance ({self.dispatch_allowance}) understates the client's "
                f"configured server-selection plus connect timeout ({configured}); the "
                "declared apply-latency bound would be shorter than a stale apply's real "
                "lifetime.",
            )

    # ....................... #

    @property
    def apply_latency_bound(self) -> timedelta:
        """Upper bound on a stale apply's whole lifetime: the client-side wait before the
        command reaches a server, plus the server-side ``maxTimeMS``. An ``updateUser`` that
        has not landed within this bound never will."""

        return self.apply_max_time + self.dispatch_allowance

    # ....................... #

    async def compose(self, tenant_id: UUID | None, *, current: str, minted: str) -> str:
        """Compose the pending URI: idle user (or same user) with the minted password."""

        user = mongo_uri_username(current)

        if self.user_pair is not None:
            if user not in self.user_pair:
                raise exc.configuration(
                    f"URI user {user!r} is not in the configured rotation user pair.",
                )

            first, second = self.user_pair
            user = second if user == first else first

        return with_mongo_credentials(current, username=user, password=minted)

    # ....................... #

    async def apply(self, tenant_id: UUID | None, pending: PendingCredential) -> None:
        """Set the pending URI's password on its user (idempotent — a retry re-applies)."""

        # Authoritative re-check: initialize() may have set the client's timeouts after this
        # target was constructed.
        self._validate_dispatch_allowance()

        uri = await self.secrets.resolve_str(pending.ref)
        user = mongo_uri_username(uri)
        password = mongo_uri_password(uri)

        if not password:
            raise exc.configuration("Pending MongoDB URI carries no password.")

        database = await self.client.db(self.user_database)
        command: JsonDict = {
            "updateUser": user,
            "pwd": password,
            # Bounds the write server-side: a command the server has not finished within
            # this is killed and never applies, which is the fence the delayed
            # reconfirmation relies on.
            "maxTimeMS": int(self.apply_max_time.total_seconds() * 1000),
        }

        await database.command(command)

    # ....................... #

    async def verify(self, tenant_id: UUID | None, pending: PendingCredential) -> None:
        """Prove the pending URI works: authenticate for real and run a command.

        pymongo connects lazily, so constructing a client proves nothing — the command is
        what forces server selection and authentication.
        """

        uri = await self.secrets.resolve_str(pending.ref)
        millis = max(1, int(self.verify_timeout.total_seconds() * 1000))
        connection: AsyncMongoClient[JsonDict] = AsyncMongoClient(
            uri,
            serverSelectionTimeoutMS=millis,
            connectTimeoutMS=millis,
        )

        try:
            await connection.admin.command("ping")

        except Exception as e:
            raise exc.infrastructure(
                f"Pending MongoDB credential failed verification; halting before promote: {e}",
                code="rotation_verify_failed",
            ) from e

        finally:
            await connection.close()
