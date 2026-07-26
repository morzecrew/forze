"""Postgres rotation target — the backend-specific steps of a DSN rotation.

The design center is the **overlap window**: promoting a credential that is not yet
valid everywhere must never strand a consumer mid-flight.

- **Dual-user alternation (the default).** Two roles (``app_a``/``app_b``); the
  secret's DSN names one. A rotation composes a DSN naming the *idle* role with a
  fresh password, sets that password on the idle role, verifies it with a real
  connection, and promotes. The previously-active role stays valid through the
  whole propagation window (TTL floor + fan-out + connection draining) and becomes
  the idle target of the *next* rotation. No moment exists where a credential in
  flight is invalid.

- **Single-role mode (degraded, explicit opt-in).** ``ALTER ROLE ... PASSWORD`` on
  the DSN's own role — Postgres holds one password per role, so between promote and
  every consumer observing the change, *new* connections with the old password
  fail. Established connections survive (passwords are connect-time-only), so with
  connect-time re-resolution the blast radius is retry noise, not an outage — but
  the mode is degraded and must be acknowledged via ``single_role_degraded=True``.

The admin client must hold ``ALTER ROLE`` on the managed roles and reach the same
cluster the rotated DSN points at. Note ``ALTER ROLE ... PASSWORD`` carries the
password as a statement literal — keep ``log_statement`` away from ``all`` on the
admin connection's server, or Postgres will log it.
"""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

import math
from datetime import timedelta
from typing import cast
from uuid import UUID

import attrs
import psycopg
from psycopg import conninfo, sql
from psycopg.abc import QueryNoTemplate

from forze.application.contracts.secrets import (
    PendingCredential,
    RotationTargetPort,
    SecretsPort,
)
from forze.base.exceptions import exc

from ..kernel.client import PostgresClientPort

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresRotationTarget(RotationTargetPort):
    """:class:`~forze.application.contracts.secrets.RotationTargetPort` for Postgres DSNs.

    The staged value is resolved through :attr:`secrets` at call time — steps carry
    ``{ref, version}`` only, never credential text. Secrets at the rotated ref must
    be libpq DSNs (URL or keyword form).
    """

    secrets: SecretsPort
    """Store the pending value is resolved from (the same store the rotator stages to)."""

    client: PostgresClientPort
    """Admin connection used for ``ALTER ROLE`` (needs privileges on the managed roles)."""

    role_pair: tuple[str, str] | None = None
    """The alternating role pair for dual-user rotation. The DSN's current user must
    be one of the two; the rotation targets the other."""

    single_role_degraded: bool = False
    """Explicit acknowledgment of single-role mode (see the module docstring for
    exactly what degrades). Mutually exclusive with :attr:`role_pair`."""

    verify_timeout: timedelta = timedelta(seconds=10)
    """Connect timeout for the verification connection."""

    apply_statement_timeout: timedelta | None = timedelta(seconds=30)
    """Server-side ``statement_timeout`` for the ``ALTER ROLE`` (``None`` disables).

    This is what makes a stale in-flight apply *boundedly* stale: a lock-losing
    worker's ALTER that the server hasn't executed within this bound is killed
    server-side and can never commit late. Keep it below the rotator's
    ``reconfirm_after`` so the delayed reconfirmation runs strictly after any
    latecomer could still land."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.role_pair is not None and self.single_role_degraded:
            raise exc.configuration(
                "Choose dual-user rotation (role_pair) or single-role mode "
                "(single_role_degraded=True), not both.",
            )

        if self.role_pair is None and not self.single_role_degraded:
            raise exc.configuration(
                "Postgres rotation needs a role_pair for dual-user alternation; "
                "single-role ALTER ROLE is degraded and requires the explicit "
                "single_role_degraded=True opt-in.",
            )

        if self.role_pair is not None and self.role_pair[0] == self.role_pair[1]:
            raise exc.configuration("Dual-user rotation needs two distinct roles.")

        if self.verify_timeout.total_seconds() <= 0:
            raise exc.configuration("Verify timeout must be positive")

        if (
            self.apply_statement_timeout is not None
            and self.apply_statement_timeout.total_seconds() <= 0
        ):
            raise exc.configuration("Apply statement timeout must be positive")

    # ....................... #

    @property
    def apply_latency_bound(self) -> timedelta | None:
        """The server-side statement timeout IS the apply-latency bound: an ALTER the
        server hasn't run within it is killed and can never commit later."""

        return self.apply_statement_timeout

    # ....................... #

    @staticmethod
    def _parse_dsn(value: str) -> dict[str, str]:
        try:
            params = conninfo.conninfo_to_dict(value)

        except Exception as e:
            raise exc.configuration(
                "Secret under rotation is not a libpq DSN.",
            ) from e

        return {key: str(item) for key, item in params.items() if item is not None}

    # ....................... #

    async def compose(self, tenant_id: UUID | None, *, current: str, minted: str) -> str:
        """Compose the pending DSN: idle role (or same role) with the minted password."""

        params = self._parse_dsn(current)
        user = params.get("user")

        if not user:
            raise exc.configuration("DSN under rotation names no user.")

        if self.role_pair is not None:
            if user not in self.role_pair:
                raise exc.configuration(
                    f"DSN user {user!r} is not in the configured rotation role pair.",
                )

            first, second = self.role_pair
            params["user"] = second if user == first else first

        params["password"] = minted

        return conninfo.make_conninfo(**params)

    # ....................... #

    async def apply(self, tenant_id: UUID | None, pending: PendingCredential) -> None:
        """Set the pending DSN's password on its role (idempotent — a retry re-applies)."""

        params = self._parse_dsn(await self.secrets.resolve_str(pending.ref))
        user = params.get("user")
        password = params.get("password")

        if not user or not password:
            raise exc.configuration("Pending DSN names no user or password.")

        alter = cast(
            QueryNoTemplate,
            sql.SQL("ALTER ROLE {} WITH PASSWORD {}").format(
                sql.Identifier(user),
                sql.Literal(password),
            ),
        )

        # Detached: a role password must never ride (or die with) an ambient
        # transaction the durable runner happens to hold open. Inside the scope,
        # transaction() opens a fresh root on its own connection.
        async with self.client.detached():
            if self.apply_statement_timeout is None:
                await self.client.execute(alter)
                return

            # SET LOCAL bounds the ALTER server-side: a stale worker's apply that
            # the server hasn't run within the bound is killed and can never
            # commit late (the fence the delayed reconfirmation relies on).
            async with self.client.transaction():
                await self.client.execute(
                    cast(
                        QueryNoTemplate,
                        sql.SQL("SET LOCAL statement_timeout = {}").format(
                            sql.Literal(int(self.apply_statement_timeout.total_seconds() * 1000))
                        ),
                    )
                )
                await self.client.execute(alter)

    # ....................... #

    async def verify(self, tenant_id: UUID | None, pending: PendingCredential) -> None:
        """Prove the pending DSN works: open a real connection and run ``SELECT 1``."""

        dsn = await self.secrets.resolve_str(pending.ref)

        try:
            connection = await psycopg.AsyncConnection.connect(
                dsn,
                # Ceil, never truncate: psycopg reads this as int(float(value)) and
                # treats <= 0 as "use the ~130s default", so a sub-second config
                # floored to 0 would turn the verify gate into a near-unbounded
                # wait. (psycopg also enforces a 2s minimum on its own.)
                connect_timeout=math.ceil(self.verify_timeout.total_seconds()),
            )

        except Exception as e:
            raise exc.infrastructure(
                f"Pending Postgres credential failed verification; halting before promote: {e}",
                code="rotation_verify_failed",
            ) from e

        try:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT 1")
                await cursor.fetchone()

        finally:
            await connection.close()
