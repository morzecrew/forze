"""Rendered ``forze schema sql`` output, one file per release (RFC 0080 §6).

Each ``<version>.sql`` holds the owned tables as that release declared them, so the upgrade
test can build a database at that release and check that ``forze schema diff`` brings it to
the current shape. Releases before ``forze schema`` had no declaration, so ``0.17.0.sql`` and
``0.24.1.sql`` were transcribed from ``pages/docs/reference/durable-schema.md`` at those tags.

A release adds its own snapshot (``CONTRIBUTING.md`` → Release Process)::

    uv run forze schema sql --deps tests.support.schema_snapshots:deps \\
        > tests/support/schema_snapshots/<version>.sql
"""

from pathlib import Path
from typing import Final

from forze_postgres import PostgresClient, PostgresDepsModule
from forze_postgres.execution.deps.configs import (
    PostgresDurableRunConfig,
    PostgresDurableScheduleConfig,
    PostgresDurableStepConfig,
)

# ----------------------- #

SNAPSHOTS: Final[Path] = Path(__file__).resolve().parent
"""The directory holding the ``<version>.sql`` files."""

deps: Final = PostgresDepsModule(
    client=PostgresClient(),
    durable_run=PostgresDurableRunConfig(relation=("public", "durable_run"), tenant_aware=True),
    durable_step=PostgresDurableStepConfig(relation=("public", "durable_step"), tenant_aware=True),
    durable_schedule=PostgresDurableScheduleConfig(
        relation=("public", "durable_schedule"), tenant_aware=True
    ),
)
"""Every owned table, tenant-aware so each index variant is rendered, under ``public``."""


def snapshot_versions() -> list[str]:
    """Every release with a snapshot, oldest first."""

    versions = [path.stem for path in SNAPSHOTS.glob("*.sql")]

    return sorted(versions, key=lambda version: tuple(int(part) for part in version.split(".")))
