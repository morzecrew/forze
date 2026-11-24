"""The Postgres store tables' declared shapes: the one copy of their DDL (RFC 0080 §5.1).

The outbox, its delivery log and the HLC checkpoint; the inbox; idempotency claims; counters;
principal epochs; and rotating credentials. ``forze schema sql`` renders these, ``forze schema
diff`` compares them with a live database, the reference pages print their render, and the
integration tests create their tables from it. A change to one of these tables is an edit
here, in the same PR as the adapter change that needs it.

A column only an opt-in feature reads, with no config switch to say whether it is on, is always
declared, nullable and not required: the idempotency ``owner`` and the counter ``expires_at``.
The adapters probe for both and serve every call that does not need them without.
"""

from typing import Final

from forze_postgres.execution.deps.configs import (
    PostgresCounterConfig,
    PostgresHlcCheckpointConfig,
    PostgresIdempotencyConfig,
    PostgresInboxConfig,
    PostgresOutboxConfig,
    PostgresPrincipalEpochConfig,
    PostgresRotatingCredentialsConfig,
)
from forze_postgres.kernel.catalog.owned import (
    OwnedColumn,
    OwnedIndex,
    OwnedRelationShape,
    Retired,
)

# ----------------------- #

_CLAIM: Final = OwnedIndex(
    keys=("outbox_route", "status", "created_at"),
    comment="the claim filters a route's pending rows and takes the oldest first",
)

_TENANT_CLAIM: Final = OwnedIndex(
    keys=("tenant_id", "outbox_route", "status", "created_at"),
    comment="on a shared tagged table a bound claim also filters tenant_id",
)

_HLC_CLAIM: Final = OwnedIndex(
    keys=("outbox_route", "status", "hlc", "created_at", "id"),
    since="0.25.0",
    comment="with hlc_ordering the claim orders by (hlc NULLS LAST, created_at, id)",
)

_TENANT_HLC_CLAIM: Final = OwnedIndex(
    keys=("tenant_id", "outbox_route", "status", "hlc", "created_at", "id"),
    since="0.25.0",
    comment="on a shared tagged table a bound causal claim also filters tenant_id",
)

_OUTBOX_RETIRED: Final = (
    Retired(
        shape=OwnedIndex(
            keys=("outbox_route", "status", "available_at", "created_at"),
            comment="the claim index outbox-schema.md printed",
        ),
        retired_in="0.25.0",
        comment="replaced by (outbox_route, status, created_at), which needs no sort",
    ),
    Retired(
        shape=OwnedIndex(
            keys=("outbox_route", "status", "available_at", "hlc", "created_at", "id"),
            comment="the causal claim index outbox-schema.md printed",
        ),
        retired_in="0.25.0",
        comment="replaced by (outbox_route, status, hlc, created_at, id), which needs no sort",
    ),
    Retired(
        shape=OwnedIndex(
            keys=("outbox_route", "status", "hlc", "created_at"),
            comment="the causal claim index the store's docstring printed",
        ),
        retired_in="0.25.0",
        comment="replaced by (outbox_route, status, hlc, created_at, id), which orders the id too",
    ),
)

# ....................... #


def outbox_shape(config: PostgresOutboxConfig) -> OwnedRelationShape:
    """The outbox table that :class:`PostgresOutboxStore` stages into and relays from.

    ``hlc_ordering`` adds ``hlc`` and claims by it, ``propagate_trace`` adds ``traceparent``,
    and ``storage="consume"`` leaves out ``published_at``, which it never writes. It keeps
    ``processing_at``: the relay's stale reclaim reads and clears it in either storage mode, and
    whether the reclaim runs is the relay's setting, which the store's config cannot see.
    """

    tagged = config.tenant_aware and not callable(config.relation)
    claim = _HLC_CLAIM if config.hlc_ordering else _CLAIM
    tenant_claim = _TENANT_HLC_CLAIM if config.hlc_ordering else _TENANT_CLAIM

    columns = [
        OwnedColumn(name="id", type="uuid", nullable=False),
        OwnedColumn(name="outbox_route", type="text", nullable=False),
        OwnedColumn(name="event_id", type="uuid", nullable=False),
        OwnedColumn(name="event_type", type="text", nullable=False),
        OwnedColumn(name="tenant_id", type="uuid"),
        OwnedColumn(name="execution_id", type="uuid"),
        OwnedColumn(name="correlation_id", type="uuid"),
        OwnedColumn(name="causation_id", type="uuid"),
        OwnedColumn(name="occurred_at", type="timestamptz", nullable=False),
        OwnedColumn(name="payload", type="jsonb", nullable=False),
        OwnedColumn(name="status", type="text", nullable=False),
        OwnedColumn(name="created_at", type="timestamptz", nullable=False),
    ]

    if config.storage == "retain":
        columns.append(OwnedColumn(name="published_at", type="timestamptz"))

    columns += [
        OwnedColumn(name="processing_at", type="timestamptz"),
        OwnedColumn(name="last_error", type="text"),
        OwnedColumn(name="attempts", type="integer", nullable=False, default="0"),
        OwnedColumn(name="available_at", type="timestamptz"),
        OwnedColumn(
            name="ordering_key",
            type="text",
            since="0.4.0",
            comment="the delivery partition key; NULL falls back to the event id",
        ),
    ]

    if config.hlc_ordering:
        columns.append(
            OwnedColumn(
                name="hlc",
                type="bigint",
                since="0.4.1",
                comment="the packed HLC stamp the causal claim orders by; NULL rows sort last",
            )
        )

    if config.propagate_trace:
        columns.append(
            OwnedColumn(
                name="traceparent",
                type="text",
                since="0.5.0",
                comment="the W3C traceparent the relay forwards; NULL carries no parent",
            )
        )

    return OwnedRelationShape(
        owner="PostgresOutboxStore",
        since="0.3.0",
        columns=tuple(columns),
        primary_key=("id",),
        indexes=(
            OwnedIndex(
                keys=("outbox_route", "event_id"),
                unique=True,
                required=True,
                comment="staging an event twice keeps one row (ON CONFLICT (outbox_route, event_id))",
            ),
            claim,
            *((tenant_claim,) if tagged else ()),
        ),
        retired=_OUTBOX_RETIRED,
    )


# ....................... #


def outbox_delivery_log_shape(config: PostgresOutboxConfig) -> OwnedRelationShape:
    """The delivery log :class:`PostgresOutboxStore` appends each consumed row to.

    The store only inserts into it. It has no key, since an event staged again after delivery
    is delivered again, and it is range-partitioned on ``published_at`` so that dropping a
    day's partition is its whole retention. The partitions are the application's.
    """

    del config  # no switch on the outbox config changes the log's shape

    return OwnedRelationShape(
        owner="PostgresOutboxStore delivery log",
        since="0.15.0",
        columns=(
            OwnedColumn(name="id", type="uuid", nullable=False),
            OwnedColumn(name="outbox_route", type="text", nullable=False),
            OwnedColumn(name="event_id", type="uuid", nullable=False),
            OwnedColumn(name="tenant_id", type="uuid"),
            OwnedColumn(name="published_at", type="timestamptz", nullable=False),
        ),
        primary_key=(),
        indexes=(
            OwnedIndex(
                keys=("event_id",),
                comment="a delivery is looked up by its event id",
            ),
        ),
        partitioned_by="published_at",
    )


# ....................... #


def hlc_checkpoint_shape(config: PostgresHlcCheckpointConfig) -> OwnedRelationShape:
    """The clock high-water mark :class:`PostgresHlcCheckpointStore` keeps, one row per node
    key. It is node-global, so it has no tenant column."""

    del config  # node_key is a row value, not a shape switch

    return OwnedRelationShape(
        owner="PostgresHlcCheckpointStore",
        since="0.5.0",
        columns=(
            OwnedColumn(name="node_key", type="text", nullable=False),
            OwnedColumn(name="hlc", type="bigint", nullable=False),
        ),
        primary_key=("node_key",),
        indexes=(),
    )


# ....................... #


def inbox_shape(config: PostgresInboxConfig) -> OwnedRelationShape:
    """The processed-message ledger :class:`PostgresInboxStore` deduplicates on. A tenant goes
    into ``inbox_route``, so a tenant-aware inbox needs no other column."""

    del config  # tenant_aware changes the stored route, not the shape

    return OwnedRelationShape(
        owner="PostgresInboxStore",
        since="0.3.0",
        columns=(
            OwnedColumn(name="inbox_route", type="text", nullable=False),
            OwnedColumn(name="message_id", type="text", nullable=False),
            OwnedColumn(name="processed_at", type="timestamptz", nullable=False),
        ),
        primary_key=("inbox_route", "message_id"),
        indexes=(),
    )


# ....................... #


def idempotency_shape(config: PostgresIdempotencyConfig) -> OwnedRelationShape:
    """The claims :class:`PostgresIdempotencyStore` records. A tenant goes into ``idem_key``,
    so a tenant-aware store needs no other column."""

    del config  # tenant_aware changes the stored key, not the shape

    return OwnedRelationShape(
        owner="PostgresIdempotencyStore",
        since="0.5.0",
        columns=(
            OwnedColumn(name="op", type="text", nullable=False),
            OwnedColumn(name="idem_key", type="text", nullable=False),
            OwnedColumn(name="payload_hash", type="text", nullable=False),
            OwnedColumn(name="status", type="text", nullable=False),
            OwnedColumn(name="result", type="bytea"),
            OwnedColumn(name="expires_at", type="timestamptz", nullable=False),
            OwnedColumn(
                name="owner",
                type="uuid",
                required=False,
                since="0.7.0",
                comment="fences a claim to the call that took it; claims go unfenced without it",
            ),
        ),
        primary_key=("op", "idem_key"),
        indexes=(),
    )


# ....................... #


def counter_shape(config: PostgresCounterConfig) -> OwnedRelationShape:
    """The sequences :class:`PostgresCounterAdapter` allocates from. ``tenant_id`` is ``''``
    when no tenant is bound, and several counter routes may share the table."""

    del config  # tenant_aware changes the stored tenant_id, not the shape

    return OwnedRelationShape(
        owner="PostgresCounterAdapter",
        since="0.5.1",
        columns=(
            OwnedColumn(name="tenant_id", type="text", nullable=False),
            OwnedColumn(name="suffix", type="text", nullable=False),
            OwnedColumn(name="value", type="bigint", nullable=False),
            OwnedColumn(
                name="expires_at",
                type="timestamptz",
                required=False,
                since="0.17.0",
                comment="only a counter bumped with a ttl reads it; NULL never expires",
            ),
        ),
        primary_key=("tenant_id", "suffix"),
        indexes=(),
    )


# ....................... #


def principal_epoch_shape(config: PostgresPrincipalEpochConfig) -> OwnedRelationShape:
    """The revocation epochs :class:`PostgresPrincipalEpochStore` keeps, one per principal."""

    del config  # no switch on the config changes the shape

    return OwnedRelationShape(
        owner="PostgresPrincipalEpochStore",
        since="0.14.0",
        columns=(
            OwnedColumn(name="principal_id", type="uuid", nullable=False),
            OwnedColumn(name="epoch", type="bigint", nullable=False),
        ),
        primary_key=("principal_id",),
        indexes=(),
    )


# ....................... #


def rotating_credentials_shape(config: PostgresRotatingCredentialsConfig) -> OwnedRelationShape:
    """The grants :class:`PostgresRotatingCredentialStore` rotates, one per ``(tenant_id,
    ref)``. ``encrypt`` changes what ``payload`` holds, not its type."""

    del config  # no switch on the config changes the shape

    return OwnedRelationShape(
        owner="PostgresRotatingCredentialStore",
        since="0.6.0",
        columns=(
            OwnedColumn(name="tenant_id", type="text", nullable=False),
            OwnedColumn(name="ref", type="text", nullable=False),
            OwnedColumn(name="payload", type="jsonb", nullable=False),
            OwnedColumn(name="expires_at", type="timestamptz"),
            OwnedColumn(name="version", type="bigint", nullable=False),
            OwnedColumn(name="burnt_reason", type="text"),
            OwnedColumn(name="created_at", type="timestamptz", nullable=False),
            OwnedColumn(name="updated_at", type="timestamptz", nullable=False),
        ),
        primary_key=("tenant_id", "ref"),
        indexes=(
            OwnedIndex(
                keys=("tenant_id", "updated_at"),
                comment="the admin listing scans a tenant's grants by updated_at",
            ),
        ),
    )
