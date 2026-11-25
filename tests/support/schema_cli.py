"""Objects ``forze schema --deps`` is pointed at in the CLI tests."""

from typing import Final

from forze_postgres import PostgresClient, PostgresDepsModule
from forze_postgres.execution.deps.configs import (
    PostgresDurableRunConfig,
    PostgresDurableScheduleConfig,
    PostgresDurableStepConfig,
    PostgresInboxConfig,
    PostgresOutboxConfig,
)
from tests.support.framework_documents import (
    document_config,
    framework_contributions,
    framework_registry,
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

shared_by_two_stores = PostgresDepsModule(
    client=PostgresClient(),
    outboxes={"events": PostgresOutboxConfig(relation=("public", "messages"))},
    inboxes={"events": PostgresInboxConfig(relation=("public", "messages"))},
)
"""An outbox and an inbox on one table, which binding refuses as a configuration error."""

CLI_SCHEMA: Final = "forze_schema_cli"
"""The schema the CLI's integration test creates its tables in, apart from every other test."""

cli_deps = PostgresDepsModule(
    client=PostgresClient(),
    durable_run=PostgresDurableRunConfig(relation=(CLI_SCHEMA, "durable_run"), tenant_aware=True),
    durable_step=PostgresDurableStepConfig(relation=(CLI_SCHEMA, "durable_step")),
    durable_schedule=PostgresDurableScheduleConfig(relation=(CLI_SCHEMA, "durable_schedule")),
)
"""Every owned table under :data:`CLI_SCHEMA`, for ``forze schema`` run end to end."""

with_documents = PostgresDepsModule(
    client=PostgresClient(),
    durable_run=PostgresDurableRunConfig(relation=("public", "durable_run")),
    rw_documents={
        "jobs": document_config("public", "jobs", tenant_aware=True),
        "authn_password_accounts": document_config("public", "authn_password_accounts"),
    },
)
"""A store table beside two framework document routes, for ``--specs``."""

specs = framework_contributions()
"""The spec registry ``--specs`` is pointed at: every framework document spec."""

frozen_specs = framework_registry()
"""The same registry, frozen, which ``--specs`` takes too."""
