"""Objects ``forze schema --deps`` is pointed at in the CLI tests."""

from typing import Final

from forze_postgres import PostgresClient, PostgresDepsModule
from forze_postgres.execution.deps.configs import (
    PostgresDurableRunConfig,
    PostgresDurableScheduleConfig,
    PostgresDurableStepConfig,
)
from tests.support.schema_snapshots import deps

# ----------------------- #

in_a_list = [object(), deps]
"""A sequence holding exactly one deps module."""

two = (deps, deps)
"""A sequence holding two, which the command refuses to choose between."""

not_deps = "a string"
"""Neither a deps module nor a sequence of them."""

tenant_scoped = PostgresDepsModule(
    client=PostgresClient(),
    durable_run=PostgresDurableRunConfig(relation=lambda tenant: ("t", "durable_run")),
    durable_step=PostgresDurableStepConfig(relation=("public", "durable_step")),
)
"""A run table resolved per tenant, which P1 skips with a warning."""

CLI_SCHEMA: Final = "forze_schema_cli"
"""The schema the CLI's integration test creates its tables in, apart from every other test."""

cli_deps = PostgresDepsModule(
    client=PostgresClient(),
    durable_run=PostgresDurableRunConfig(relation=(CLI_SCHEMA, "durable_run"), tenant_aware=True),
    durable_step=PostgresDurableStepConfig(relation=(CLI_SCHEMA, "durable_step")),
    durable_schedule=PostgresDurableScheduleConfig(relation=(CLI_SCHEMA, "durable_schedule")),
)
"""Every owned table under :data:`CLI_SCHEMA`, for ``forze schema`` run end to end."""
