"""Rendered ``forze schema sql`` output, one file per release (RFC 0080 §6).

Each ``<version>.sql`` holds the owned tables as that release declared them, so the upgrade
test can build a database at that release and check that ``forze schema diff`` brings it to
the current shape. Releases before ``forze schema`` had no declaration, so ``0.17.0.sql`` and
``0.24.1.sql`` were transcribed from the reference pages at those tags: the durable tables
from ``durable-schema.md``, and in ``0.24.1.sql`` the store tables from ``outbox-schema.md``
and the store adapters' docstrings. A table a snapshot lacks is created by the diff.

A release adds its own snapshot (``CONTRIBUTING.md`` → Release Process)::

    uv run forze schema sql --deps tests.support.schema_snapshots:deps \\
        > tests/support/schema_snapshots/<version>.sql
"""

from pathlib import Path
from typing import Any, Final

from forze_postgres import PostgresClient, PostgresDepsModule
from forze_postgres.execution.deps.configs import (
    PostgresCounterConfig,
    PostgresDurableRunConfig,
    PostgresDurableScheduleConfig,
    PostgresDurableStepConfig,
    PostgresHlcCheckpointConfig,
    PostgresIdempotencyConfig,
    PostgresInboxConfig,
    PostgresOutboxConfig,
    PostgresPrincipalEpochConfig,
    PostgresRotatingCredentialsConfig,
)

# ----------------------- #

SNAPSHOTS: Final[Path] = Path(__file__).resolve().parent
"""The directory holding the ``<version>.sql`` files."""

_NO_EXCHANGE: Any = object()
"""Stands in for the token endpoint: no shape reads it."""


def deps_for(schema: str) -> PostgresDepsModule:
    """Every owned table under *schema*, with every switch that adds an index or a column on:
    tenant-aware tables, and an outbox with ``hlc_ordering`` and ``propagate_trace``. A second
    outbox consumes its rows into a delivery log."""

    def relation(name: str) -> tuple[str, str]:
        return (schema, name)

    return PostgresDepsModule(
        client=PostgresClient(),
        durable_run=PostgresDurableRunConfig(relation=relation("durable_run"), tenant_aware=True),
        durable_step=PostgresDurableStepConfig(
            relation=relation("durable_step"), tenant_aware=True
        ),
        durable_schedule=PostgresDurableScheduleConfig(
            relation=relation("durable_schedule"), tenant_aware=True
        ),
        outboxes={
            "events": PostgresOutboxConfig(
                relation=relation("outbox"),
                tenant_aware=True,
                hlc_ordering=True,
                propagate_trace=True,
            ),
            "consumed": PostgresOutboxConfig(
                relation=relation("outbox_consumed"),
                storage="consume",
                delivery_log=relation("outbox_delivered"),
            ),
        },
        hlc_checkpoint=PostgresHlcCheckpointConfig(relation=relation("hlc_checkpoint")),
        inboxes={"events": PostgresInboxConfig(relation=relation("inbox"))},
        idempotencies={"commands": PostgresIdempotencyConfig(relation=relation("idempotency"))},
        counters={"sequences": PostgresCounterConfig(relation=relation("counters"))},
        principal_epochs=PostgresPrincipalEpochConfig(relation=relation("principal_epochs")),
        rotating_credentials=PostgresRotatingCredentialsConfig(
            relation=relation("rotating_credentials"), exchanger=_NO_EXCHANGE
        ),
    )


deps: Final = deps_for("public")
"""Every owned table under ``public``, each index variant rendered."""


def snapshot_versions() -> list[str]:
    """Every release with a snapshot, oldest first."""

    versions = [path.stem for path in SNAPSHOTS.glob("*.sql")]

    return sorted(versions, key=lambda version: tuple(int(part) for part in version.split(".")))
