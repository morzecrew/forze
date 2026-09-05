"""A declared isolation floor covers the durable routes on Mongo too.

The Postgres mirror is `tests/unit/test_forze_postgres/test_pg_deps_durable_tenancy.py`; the
two modules build their route lists differently (groups here, hand-built specs there), which
is precisely why each needs its own leg — a floor that covers durable on one module and not
the other is the shape this whole change exists to remove.

# covers: MongoDepsModule.__attrs_post_init__
"""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException
from forze_mongo.execution.deps import MongoDepsModule
from forze_mongo.execution.deps.configs import (
    MongoDocumentConfig,
    MongoDurableRunConfig,
    MongoDurableScheduleConfig,
    MongoDurableStepConfig,
)
from forze_mongo.kernel.client import MongoClient

# ----------------------- #


def _module(**extra: object) -> MongoDepsModule:
    """A module that clears a ``tagged`` floor on everything but *extra*."""

    return MongoDepsModule(
        client=MongoClient(),
        required_tenant_isolation="tagged",
        rw_documents={
            "orders": MongoDocumentConfig(
                read=("app", "orders"),
                write=("app", "orders"),
                tenant_aware=True,
            ),
        },
        **extra,  # type: ignore[arg-type]
    )


def _configs() -> dict[str, object]:
    return {
        "durable_step": MongoDurableStepConfig(collection=("app", "durable_step")),
        "durable_run": MongoDurableRunConfig(collection=("app", "durable_run")),
        "durable_schedule": MongoDurableScheduleConfig(collection=("app", "durable_schedule")),
    }


def _aware_configs() -> dict[str, object]:
    return {
        "durable_step": MongoDurableStepConfig(
            collection=("app", "durable_step"), tenant_aware=True
        ),
        "durable_run": MongoDurableRunConfig(collection=("app", "durable_run"), tenant_aware=True),
        "durable_schedule": MongoDurableScheduleConfig(
            collection=("app", "durable_schedule"), tenant_aware=True
        ),
    }


# ....................... #


def test_the_compliant_module_passes() -> None:
    """The control: every failure below is the durable config, not the fixture."""

    assert _module() is not None


@pytest.mark.parametrize("field", ["durable_step", "durable_run", "durable_schedule"])
def test_a_durable_route_below_the_floor_is_refused(field: str) -> None:
    with pytest.raises(CoreException, match="mongo_tenancy_validation_failed"):
        _module(**{field: _configs()[field]})


@pytest.mark.parametrize("field", ["durable_step", "durable_run", "durable_schedule"])
def test_a_tenant_aware_durable_route_clears_the_floor(field: str) -> None:
    assert _module(**{field: _aware_configs()[field]}) is not None
