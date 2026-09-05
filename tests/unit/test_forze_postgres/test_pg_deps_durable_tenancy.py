"""A declared isolation floor covers the durable routes, or it describes less than it says.

The floor is a statement about the module. Before the durable configs were registered as
routes, a deployment could declare ``tagged``, wire a durable run store with
``tenant_aware=False``, and be told nothing — the tier came from the other planes. These
tests are what makes the declaration total.

# covers: PostgresDepsModule.__attrs_post_init__
"""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException
from forze_postgres.execution.deps import PostgresDepsModule, PostgresDocumentConfig
from forze_postgres.execution.deps.configs import (
    PostgresDurableRunConfig,
    PostgresDurableScheduleConfig,
    PostgresDurableStepConfig,
)
from forze_postgres.kernel.client import PostgresClient

# ----------------------- #


def _module(**extra: object) -> PostgresDepsModule:
    """A module that clears a ``tagged`` floor on everything but *extra*."""

    return PostgresDepsModule(
        client=PostgresClient(),
        required_tenant_isolation="tagged",
        rw_documents={
            "orders": PostgresDocumentConfig(
                read=("public", "orders"),
                write=("public", "orders"),
                bookkeeping_strategy="application",
                tenant_aware=True,
            ),
        },
        **extra,  # type: ignore[arg-type]
    )


# ....................... #


def test_the_compliant_module_passes() -> None:
    """The control: without a durable route the floor is met, so every failure below is the
    durable config and not the fixture."""

    assert _module() is not None


@pytest.mark.parametrize(
    "field",
    ["durable_step", "durable_run", "durable_schedule"],
)
def test_a_durable_route_below_the_floor_is_refused(field: str) -> None:
    configs = {
        "durable_step": PostgresDurableStepConfig(relation=("public", "durable_step")),
        "durable_run": PostgresDurableRunConfig(relation=("public", "durable_run")),
        "durable_schedule": PostgresDurableScheduleConfig(relation=("public", "durable_schedule")),
    }

    with pytest.raises(CoreException, match="postgres_tenancy_validation_failed"):
        _module(**{field: configs[field]})


@pytest.mark.parametrize(
    "field",
    ["durable_step", "durable_run", "durable_schedule"],
)
def test_a_tenant_aware_durable_route_clears_the_floor(field: str) -> None:
    configs = {
        "durable_step": PostgresDurableStepConfig(
            relation=("public", "durable_step"), tenant_aware=True
        ),
        "durable_run": PostgresDurableRunConfig(
            relation=("public", "durable_run"), tenant_aware=True
        ),
        "durable_schedule": PostgresDurableScheduleConfig(
            relation=("public", "durable_schedule"), tenant_aware=True
        ),
    }

    assert _module(**{field: configs[field]}) is not None
