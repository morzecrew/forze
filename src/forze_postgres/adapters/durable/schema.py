"""The durable tables' declared shapes: the one copy of their DDL (RFC 0080 §5.1).

``forze schema sql`` renders these, ``forze schema diff`` compares them with a live database,
``pages/docs/reference/durable-schema.md`` prints their render, and the integration tests
create their tables from it. A change to a durable table's shape is an edit here, in the same
PR as the adapter change that needs it.

A tenant-aware table on a static relation is a shared tagged table, so a bound scan also
filters ``tenant_id``: its shape adds the ``tenant_id``-leading variants of the scan indexes.
It keeps the plain ones, which the unbound recovery scan reads.
"""

from typing import Final

from forze_postgres.execution.deps.configs import (
    PostgresDurableRunConfig,
    PostgresDurableScheduleConfig,
    PostgresDurableStepConfig,
)
from forze_postgres.kernel.catalog.owned import (
    OwnedColumn,
    OwnedIndex,
    OwnedRelationShape,
    Retired,
)

# ----------------------- #

_RUN_COLUMNS: Final = (
    OwnedColumn(name="run_id", type="text", nullable=False),
    OwnedColumn(name="name", type="text", nullable=False),
    OwnedColumn(name="status", type="text", nullable=False),
    OwnedColumn(name="idempotency_key", type="text"),
    OwnedColumn(
        name="coalesce_key",
        type="text",
        required=False,
        comment="only enqueue(coalesce_key=...) reads it, so add it before the first such call",
    ),
    OwnedColumn(name="input", type="jsonb"),
    OwnedColumn(name="output", type="jsonb"),
    OwnedColumn(name="error", type="text"),
    OwnedColumn(name="tenant_id", type="uuid"),
    OwnedColumn(name="attempts", type="integer", nullable=False, default="0"),
    OwnedColumn(name="leased_until", type="timestamptz"),
    OwnedColumn(name="available_at", type="timestamptz"),
    OwnedColumn(name="created_at", type="timestamptz", nullable=False),
    OwnedColumn(name="updated_at", type="timestamptz", nullable=False),
    OwnedColumn(
        name="cancel_requested_at",
        type="timestamptz",
        comment="since 0.6.0: starting a run, the recovery scan and every read return it",
    ),
    OwnedColumn(
        name="cancel_refused_at",
        type="timestamptz",
        comment="since 0.6.0: starting a run, the recovery scan and every read return it",
    ),
)

_RECOVERY_SCAN: Final = OwnedIndex(
    keys=("status", "name", "created_at"),
    comment="recommended since 0.18.0: the recovery scan (claim_abandoned) filters on status and name",
)

_TENANT_RECOVERY_SCAN: Final = OwnedIndex(
    keys=("tenant_id", "status", "name", "created_at"),
    comment="recommended since 0.18.0: on a shared tagged table a bound scan also filters tenant_id",
)

_PURGE: Final = OwnedIndex(
    keys=("status", "updated_at"),
    comment="recommended where runs are purged: purge_finished filters on status and updated_at",
)

_LISTING: Final = OwnedIndex(
    keys=("created_at DESC", "run_id DESC"),
    comment="recommended: the admin listing (list_runs) orders newest first and seeks on it",
)

_TENANT_LISTING: Final = OwnedIndex(
    keys=("tenant_id", "created_at DESC", "run_id DESC"),
    comment="recommended: on a shared tagged table a bound listing also filters tenant_id",
)

_RUN_REQUIRED: Final = (
    OwnedIndex(
        keys=("idempotency_key",),
        unique=True,
        required=True,
        comment="two submits of one key converge on one run (ON CONFLICT (idempotency_key))",
    ),
    OwnedIndex(
        keys=("coalesce_key",),
        unique=True,
        where="status = 'pending' AND attempts = 0",
        required=True,
        comment="enqueue(coalesce_key=...) joins the one unclaimed pending run per key",
    ),
)

_RUN_RETIRED: Final = (
    Retired(
        shape=OwnedIndex(keys=("status", "created_at"), comment="the 0.17 recovery scan"),
        comment="retired in 0.18.0, replaced by (status, name, created_at)",
    ),
    Retired(
        shape=OwnedIndex(
            keys=("tenant_id", "status", "created_at"),
            comment="the 0.17 bound recovery scan",
        ),
        comment="retired in 0.18.0, replaced by (tenant_id, status, name, created_at)",
    ),
)

# ....................... #


def durable_run_shape(config: PostgresDurableRunConfig) -> OwnedRelationShape:
    """The ``durable_run`` table that :class:`PostgresDurableRunStore` reads and writes."""

    tagged = config.tenant_aware and not callable(config.relation)
    scans = (
        (_RECOVERY_SCAN, _TENANT_RECOVERY_SCAN, _PURGE, _LISTING, _TENANT_LISTING)
        if tagged
        else (_RECOVERY_SCAN, _PURGE, _LISTING)
    )

    return OwnedRelationShape(
        owner="PostgresDurableRunStore",
        columns=_RUN_COLUMNS,
        primary_key=("run_id",),
        indexes=(*_RUN_REQUIRED, *scans),
        retired=_RUN_RETIRED,
    )


# ....................... #


def durable_step_shape(config: PostgresDurableStepConfig) -> OwnedRelationShape:
    """The ``durable_step`` journal that :class:`PostgresDurableFunctionStepAdapter` keeps.

    Its primary key is the journal's dedup key, and a purge deletes by ``run_id``, which the
    key leads with, so it declares no other index.
    """

    del config  # no switch on the step config changes the journal's shape

    return OwnedRelationShape(
        owner="PostgresDurableFunctionStepAdapter",
        columns=(
            OwnedColumn(name="run_id", type="text", nullable=False),
            OwnedColumn(name="step_id", type="text", nullable=False),
            OwnedColumn(name="result", type="jsonb", nullable=False),
            OwnedColumn(name="tenant_id", type="uuid"),
            OwnedColumn(name="created_at", type="timestamptz", nullable=False),
        ),
        primary_key=("run_id", "step_id"),
        indexes=(),
    )


# ....................... #


def durable_schedule_shape(config: PostgresDurableScheduleConfig) -> OwnedRelationShape:
    """The ``durable_schedule`` table that :class:`PostgresDurableScheduleStore` reads and
    writes."""

    tagged = config.tenant_aware and not callable(config.relation)
    due = OwnedIndex(
        keys=("next_fire_at",),
        where="enabled",
        comment="recommended: claim_due scans enabled schedules due now, oldest first",
    )
    tenant_due = OwnedIndex(
        keys=("tenant_id", "next_fire_at"),
        where="enabled",
        comment="recommended: on a shared tagged table a bound scheduler also filters tenant_id",
    )

    return OwnedRelationShape(
        owner="PostgresDurableScheduleStore",
        columns=(
            OwnedColumn(name="schedule_id", type="text", nullable=False),
            OwnedColumn(name="name", type="text", nullable=False),
            OwnedColumn(name="cron", type="text", nullable=False),
            OwnedColumn(name="tz", type="text"),
            OwnedColumn(name="input", type="jsonb"),
            OwnedColumn(name="next_fire_at", type="timestamptz", nullable=False),
            OwnedColumn(name="enabled", type="boolean", nullable=False, default="true"),
            OwnedColumn(name="tenant_id", type="uuid"),
            OwnedColumn(name="created_at", type="timestamptz", nullable=False),
            OwnedColumn(name="updated_at", type="timestamptz", nullable=False),
        ),
        primary_key=("schedule_id",),
        indexes=(due, tenant_due) if tagged else (due,),
    )
