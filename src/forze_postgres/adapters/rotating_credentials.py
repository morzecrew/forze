"""Postgres store for credentials a counterparty rotates as a side effect of use.

The plane's two hard requirements land on two Postgres mechanisms:

- **Serialize the exchange.** ``SELECT … FOR UPDATE`` on the credential's row, taken
  *before* the counterparty is called and held until the replacement is committed. A second
  worker — in this process or another — blocks on that row, and when it proceeds it re-reads
  a version that has moved on, so it converges on the winner instead of presenting a token
  the counterparty has already burned. An in-process stripe of locks sits in front so
  same-process racers never even reach the database.
- **Persist before use.** The exchange happens *inside* the row-locked transaction and its
  ``COMMIT`` is what makes the replacement durable, so nothing observes a credential that
  is not committed. A write or commit that fails after a successful exchange is reported as
  a lost credential rather than a retryable storage error — the presented token is already
  burned by then, and no retry can bring it back.

Holding a row lock across a third-party call is deliberate, and it is bounded on both
sides. :attr:`exchange_timeout` caps the call in Python; the transaction additionally sets
``idle_in_transaction_session_timeout`` and ``lock_timeout`` for its own duration, derived
from that bound. Without the first, a server-side idle reaper could kill the transaction
*between* a successful exchange and its commit — manufacturing precisely the lockout this
plane exists to prevent.

The table is provided by the application; expected schema::

    CREATE TABLE <relation> (
        tenant_id    text        NOT NULL,
        ref          text        NOT NULL,
        payload      jsonb       NOT NULL,
        expires_at   timestamptz,
        version      bigint      NOT NULL,
        burnt_reason text,
        created_at   timestamptz NOT NULL,
        updated_at   timestamptz NOT NULL,
        PRIMARY KEY (tenant_id, ref)
    );

The primary key is the only index the store needs — every access is a point lookup on
``(tenant_id, ref)``. ``tenant_id`` is part of that key rather than a filter beside it: a
table keyed on ``ref`` alone would hand one tenant another's grant. An unbound tenant
stores as the empty string.

``payload`` holds the credential itself (``access_token``, ``refresh_token``,
``metadata``); ``expires_at`` is lifted out as a column so an operator can find grants
about to expire without reading secrets. Both tokens are stored in the clear — protect this
table the way you protect the credentials it holds, and keep it out of logical backups that
travel.
"""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

import asyncio
from datetime import datetime, timedelta
from typing import Final, cast, final
from uuid import UUID

import attrs
from psycopg import sql
from psycopg.types.json import Jsonb

from forze.application.contracts.secrets import (
    BURNT_CREDENTIAL_CODE,
    CREDENTIAL_EXCHANGE_TIMEOUT_CODE,
    CREDENTIAL_PERSIST_LOST_CODE,
    INVALID_GRANT_CODE,
    CredentialExchangerPort,
    ExchangedCredential,
    RotatingCredential,
    RotatingCredentialStorePort,
    SecretRef,
    SecretVersion,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import CoreException, exc
from forze.base.logging import get_logger
from forze.base.primitives import JsonDict, StripedAsyncLocks, utcnow
from forze_postgres.kernel.client import PostgresClientPort
from forze_postgres.kernel.gateways.base import PostgresQualifiedName
from forze_postgres.kernel.relation import (
    RelationSpec,
    coerce_relation_spec,
    resolve_postgres_qname,
)

# ----------------------- #

_TRANSACTION_BOUND_FACTOR: Final[int] = 2
"""Multiple of :attr:`~PostgresRotatingCredentialStore.exchange_timeout` used for the
transaction's server-side bounds.

Both bounds must *exceed* the exchange, never merely match it:

- ``idle_in_transaction_session_timeout`` — the transaction sits idle while the exchange is
  in flight, and being reaped there would lose an already-burned credential;
- ``lock_timeout`` — a racer should be able to wait out one full exchange *and its commit*
  and then converge on the winner, which is a better outcome than erroring.
"""

_ROW_COLUMNS: Final[str] = "payload, expires_at, version, burnt_reason"

log = get_logger(__name__)


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresRotatingCredentialStore(TenancyMixin, RotatingCredentialStorePort):
    """:class:`RotatingCredentialStorePort` over one Postgres table (see the module docstring)."""

    client: PostgresClientPort
    """Client owning the connection the row lock is taken on."""

    relation: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Schema-qualified table holding one document per ``(tenant_id, ref)``."""

    exchanger: CredentialExchangerPort
    """The counterparty call. Invoked only while the row lock is held."""

    exchange_timeout: timedelta = timedelta(seconds=30)
    """Bound on the counterparty call, and the source of the transaction's own bounds.

    An unbounded exchange would hold a row lock — and a pooled connection — for as long as
    the provider is willing to stall."""

    _locks: StripedAsyncLocks = attrs.field(factory=StripedAsyncLocks, init=False, repr=False)
    """First line of serialization: collapses same-process racers before any of them checks
    out a connection to queue on the row lock."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.exchange_timeout.total_seconds() <= 0:
            raise exc.configuration(
                "Exchange timeout must be positive; an unbounded exchange holds a row "
                "lock and a pooled connection for as long as the counterparty stalls.",
            )

    # ....................... #

    async def _table(self) -> PostgresQualifiedName:
        return await resolve_postgres_qname(self.relation, self._tenant_id_for_resolve())

    # ....................... #

    def _tenant_key(self) -> str:
        # Part of the primary key, so it must be a value and never NULL — an unbound tenant
        # is the empty string, matching the counter store's convention.
        tenant: UUID | None = self._tenant_id_for_resolve()

        return "" if tenant is None else str(tenant)

    # ....................... #

    async def _bound_transaction(self) -> None:
        """Bound the row-locked transaction server-side, for its duration only.

        ``SET LOCAL`` scopes both settings to this transaction, so a deployment's global
        reaper policy is untouched — and this transaction is guaranteed to outlive its own
        bounded exchange no matter how aggressive that policy is.
        """

        bound = int(self.exchange_timeout.total_seconds() * 1000) * _TRANSACTION_BOUND_FACTOR

        await self.client.execute(
            sql.SQL("SET LOCAL idle_in_transaction_session_timeout = {}").format(
                sql.Literal(bound)
            ),
        )
        await self.client.execute(
            sql.SQL("SET LOCAL lock_timeout = {}").format(sql.Literal(bound)),
        )

    # ....................... #

    @staticmethod
    def _payload(row: JsonDict) -> dict[str, object]:
        payload = row["payload"]

        return cast(dict[str, object], payload) if isinstance(payload, dict) else {}

    # ....................... #

    @staticmethod
    def _metadata(payload: dict[str, object]) -> dict[str, str]:
        metadata = payload.get("metadata")

        if not isinstance(metadata, dict):
            return {}

        return {str(key): str(value) for key, value in cast(dict[object, object], metadata).items()}

    # ....................... #

    @classmethod
    def _view(cls, row: JsonDict) -> RotatingCredential:
        """Project a stored row to the caller-facing view (no refresh token)."""

        payload = cls._payload(row)
        expires_at = row["expires_at"]

        return RotatingCredential(
            access_token=str(payload.get("access_token", "")),
            version=SecretVersion(str(row["version"])),
            expires_at=expires_at if isinstance(expires_at, datetime) else None,
            metadata=cls._metadata(payload),
        )

    # ....................... #

    @staticmethod
    def _guard_live(row: JsonDict | None, ref: SecretRef) -> JsonDict:
        if row is None:
            raise exc.not_found(f"No rotating credential stored at {ref.path!r}")

        reason = row["burnt_reason"]

        if reason is not None:
            raise exc.precondition(
                f"Grant at {ref.path!r} is burnt and needs re-authorization: {reason}",
                code=BURNT_CREDENTIAL_CODE,
                details={"ref": ref.path},
            )

        return row

    # ....................... #

    async def _read(
        self,
        table: PostgresQualifiedName,
        tenant: str,
        ref: SecretRef,
        *,
        for_update: bool,
    ) -> JsonDict | None:
        statement = sql.SQL(
            "SELECT {columns} FROM {table} WHERE tenant_id = {tenant} AND ref = {ref}"
        ).format(
            columns=sql.SQL(_ROW_COLUMNS),
            table=table.ident(),
            tenant=sql.Placeholder("tenant"),
            ref=sql.Placeholder("ref"),
        )

        if for_update:
            # Requires the surrounding transaction: outside one the lock would be released
            # before the exchange even starts, which is the whole window this store closes.
            self.client.require_transaction()
            statement = statement + sql.SQL(" FOR UPDATE")

        return await self.client.fetch_one(statement, {"tenant": tenant, "ref": ref.path})

    # ....................... #

    async def _persist(
        self,
        table: PostgresQualifiedName,
        tenant: str,
        ref: SecretRef,
        credential: ExchangedCredential,
        *,
        version: int,
    ) -> JsonDict:
        """Write the replacement, clearing any burn notice by construction."""

        now = utcnow()
        payload: JsonDict = {
            "access_token": credential.access_token,
            "refresh_token": credential.refresh_token,
            "metadata": {str(key): str(value) for key, value in credential.metadata.items()},
        }
        row = await self.client.fetch_one(
            sql.SQL(
                """
                INSERT INTO {table}
                    (tenant_id, ref, payload, expires_at, version, burnt_reason,
                     created_at, updated_at)
                VALUES
                    ({tenant}, {ref}, {payload}, {expires_at}, {version}, NULL, {now}, {now})
                ON CONFLICT (tenant_id, ref) DO UPDATE SET
                    payload = EXCLUDED.payload,
                    expires_at = EXCLUDED.expires_at,
                    version = EXCLUDED.version,
                    burnt_reason = NULL,
                    updated_at = EXCLUDED.updated_at
                RETURNING {columns}
                """
            ).format(
                table=table.ident(),
                columns=sql.SQL(_ROW_COLUMNS),
                tenant=sql.Placeholder("tenant"),
                ref=sql.Placeholder("ref"),
                payload=sql.Placeholder("payload"),
                expires_at=sql.Placeholder("expires_at"),
                version=sql.Placeholder("version"),
                now=sql.Placeholder("now"),
            ),
            {
                "tenant": tenant,
                "ref": ref.path,
                "payload": Jsonb(payload),
                "expires_at": credential.expires_at,
                "version": version,
                "now": now,
            },
        )

        if row is None:  # pragma: no cover — an upsert with RETURNING always yields a row
            raise exc.internal(f"Rotating credential upsert at {ref.path!r} returned no row.")

        return row

    # ....................... #

    async def _mark_burnt(
        self,
        table: PostgresQualifiedName,
        tenant: str,
        ref: SecretRef,
        reason: str,
    ) -> None:
        """Record the burn notice, inserting a placeholder when no grant was ever stored.

        A notice for an unknown ref still has to stick: the caller learned the grant is
        dead, and a later read must report *needs re-authorization* rather than a bare
        "not found".
        """

        now = utcnow()

        await self.client.execute(
            sql.SQL(
                """
                INSERT INTO {table}
                    (tenant_id, ref, payload, expires_at, version, burnt_reason,
                     created_at, updated_at)
                VALUES
                    ({tenant}, {ref}, {empty}, NULL, 0, {reason}, {now}, {now})
                ON CONFLICT (tenant_id, ref) DO UPDATE SET
                    burnt_reason = EXCLUDED.burnt_reason,
                    updated_at = EXCLUDED.updated_at
                """
            ).format(
                table=table.ident(),
                tenant=sql.Placeholder("tenant"),
                ref=sql.Placeholder("ref"),
                empty=sql.Placeholder("empty"),
                reason=sql.Placeholder("reason"),
                now=sql.Placeholder("now"),
            ),
            {
                "tenant": tenant,
                "ref": ref.path,
                "empty": Jsonb({}),
                "reason": reason,
                "now": now,
            },
        )

    # ....................... #

    async def _exchange(self, ref: SecretRef, row: JsonDict) -> ExchangedCredential:
        """Run the bounded counterparty call."""

        payload = self._payload(row)

        try:
            async with asyncio.timeout(self.exchange_timeout.total_seconds()):
                return await self.exchanger.exchange(
                    ref,
                    refresh_token=str(payload.get("refresh_token", "")),
                    metadata=self._metadata(payload),
                )

        except TimeoutError as e:
            # Transient: we never learned whether the counterparty processed the request,
            # so the transaction rolls back and the stored credential stands.
            raise exc.infrastructure(
                f"Credential exchange for {ref.path!r} exceeded {self.exchange_timeout}.",
                code=CREDENTIAL_EXCHANGE_TIMEOUT_CODE,
                details={"ref": ref.path},
            ) from e

    # ....................... #

    async def get(self, ref: SecretRef) -> RotatingCredential:
        table = await self._table()
        row = await self._read(table, self._tenant_key(), ref, for_update=False)

        return self._view(self._guard_live(row, ref))

    # ....................... #

    async def refresh(self, ref: SecretRef, *, observed: SecretVersion) -> RotatingCredential:
        tenant = self._tenant_key()

        async with self._locks.for_key(f"{tenant}|{ref.path}"):
            return await self._rotate_under_row_lock(tenant, ref, observed)

    # ....................... #

    async def _rotate_under_row_lock(
        self,
        tenant: str,
        ref: SecretRef,
        observed: SecretVersion,
    ) -> RotatingCredential:
        """Exchange and commit under the row lock, classifying every way it can end.

        The three non-happy endings are genuinely different and must not be collapsed:
        a *stale* caller converges silently, a *dead grant* commits its burn notice and
        then raises, and a *failed persist after a successful exchange* is unrecoverable.
        """

        table = await self._table()
        exchanged = False
        credential: ExchangedCredential | None = None
        burn_reason: str | None = None
        rotated: RotatingCredential | None = None

        try:
            # Detached: the row lock and the bounded transaction must be this store's own
            # root, never a savepoint inside whatever transaction the caller happens to
            # hold — a nested scope cannot bound the session, and a caller's rollback would
            # discard a credential the counterparty has already burned.
            async with self.client.detached(), self.client.transaction():
                await self._bound_transaction()

                row = self._guard_live(await self._read(table, tenant, ref, for_update=True), ref)
                current = SecretVersion(str(row["version"]))

                if current != observed:
                    # Single-flight: the version moved while we queued on the row lock, so
                    # another worker already exchanged. Presenting the stored token again
                    # would be reuse, and reuse detection can revoke the whole family.
                    return self._view(row)

                try:
                    credential = await self._exchange(ref, row)
                    exchanged = True

                except CoreException as e:
                    if e.code != INVALID_GRANT_CODE:
                        raise

                    # Hold the reason and commit the notice below: raising from inside the
                    # transaction would roll it back and lose the one fact worth keeping.
                    burn_reason = e.summary

                if burn_reason is not None:
                    await self._mark_burnt(table, tenant, ref, burn_reason)

                elif credential is not None:
                    rotated = self._view(
                        await self._persist(
                            table,
                            tenant,
                            ref,
                            credential,
                            version=int(str(row["version"])) + 1,
                        )
                    )

        except Exception as e:
            if not exchanged:
                # Nothing was consumed at the counterparty (or the burn notice itself
                # failed to store): an ordinary failure, and the credential is intact.
                raise

            # The counterparty already burned the presented token and this frame holds the
            # only copy of the replacement, so the grant is gone. Say exactly that — a
            # generic storage error would read as retryable, and no retry can help.
            log.critical(
                "rotating credential lost after a successful exchange",
                ref=ref.path,
                error=str(e),
            )

            raise exc.internal(
                f"Exchanged credential for {ref.path!r} could not be committed; the "
                "presented token is already burned, so this grant needs re-authorization.",
                code=CREDENTIAL_PERSIST_LOST_CODE,
                details={"ref": ref.path},
            ) from e

        if burn_reason is not None:
            raise exc.precondition(
                f"Counterparty permanently rejected the grant at {ref.path!r}; "
                f"re-authorization required: {burn_reason}",
                code=BURNT_CREDENTIAL_CODE,
                details={"ref": ref.path},
            )

        if rotated is None:  # pragma: no cover — set on every path that reaches here
            raise exc.internal(f"Rotation of {ref.path!r} ended without a credential.")

        # Only reached after a successful COMMIT, which is what makes the replacement
        # durable: no caller ever observes a credential that is not already persisted.
        return rotated

    # ....................... #

    async def put(self, ref: SecretRef, credential: ExchangedCredential) -> RotatingCredential:
        tenant = self._tenant_key()

        async with self._locks.for_key(f"{tenant}|{ref.path}"):
            table = await self._table()

            async with self.client.detached(), self.client.transaction():
                await self._bound_transaction()

                row = await self._read(table, tenant, ref, for_update=True)
                version = 0 if row is None else int(str(row["version"]))

                return self._view(
                    await self._persist(table, tenant, ref, credential, version=version + 1)
                )

    # ....................... #

    async def burn(self, ref: SecretRef, *, reason: str) -> None:
        tenant = self._tenant_key()

        async with self._locks.for_key(f"{tenant}|{ref.path}"):
            table = await self._table()

            await self._mark_burnt(table, tenant, ref, reason)
