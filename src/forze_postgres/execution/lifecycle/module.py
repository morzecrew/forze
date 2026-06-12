"""Postgres lifecycle module (pool, catalog warmup, schema validation)."""

from typing import Sequence, final

import attrs
from pydantic import SecretStr

from forze.application.contracts.execution import LifecycleStep
from forze.application.execution.lifecycle import LifecycleModule
from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKeyMapping

from ...kernel.catalog.validation.validate_schema import PostgresDocumentSchemaSpec
from ...kernel.client import (
    PostgresClientPort,
    PostgresConfig,
    RoutedPostgresClient,
)
from ..deps.configs import (
    PostgresFederatedSearchConfig,
    PostgresHubSearchConfig,
    PostgresSearchConfig,
)
from .catalog_warmup import postgres_catalog_warmup_lifecycle_step
from .document_schema import postgres_document_schema_validation_lifecycle_step
from .pool import postgres_lifecycle_step, routed_postgres_lifecycle_step

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PostgresLifecycleModule(LifecycleModule):
    """Lifecycle module for Postgres client startup and optional follow-up steps."""

    client: PostgresClientPort
    """Pre-constructed client (single-DSN or routed; shared with :class:`PostgresDepsModule`)."""

    dsn: str | SecretStr | None = attrs.field(default=None, repr=False)
    """Connection DSN when :attr:`client` is not routed (required for non-routed clients)."""

    config: PostgresConfig = attrs.field(factory=PostgresConfig, repr=False)
    """Pool configuration for non-routed :func:`postgres_lifecycle_step`."""

    pool_step_name: str = "postgres_lifecycle"
    """Step id for the pool lifecycle step."""

    warmup_step_name: str = "postgres_catalog_warmup"
    """Step id for catalog warmup."""

    schema_step_name: str = "postgres_document_schema_validate"
    """Step id for document schema validation."""

    searches: StrKeyMapping[PostgresSearchConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """When set, registers catalog warmup for these search routes."""

    hub_searches: StrKeyMapping[PostgresHubSearchConfig] | None = attrs.field(
        default=None,
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """When set, registers catalog warmup for hub search routes."""

    federated_searches: StrKeyMapping[PostgresFederatedSearchConfig] | None = (
        attrs.field(
            default=None,
            converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
        )
    )
    """When set, registers catalog warmup for federated search routes."""

    schema_specs: Sequence[PostgresDocumentSchemaSpec] = attrs.field(factory=tuple)
    """When non-empty, registers document schema validation at startup."""

    # ....................... #

    def __call__(self) -> tuple[LifecycleStep, ...]:
        steps: list[LifecycleStep] = []

        if isinstance(self.client, RoutedPostgresClient):
            steps.append(
                routed_postgres_lifecycle_step(
                    self.pool_step_name,
                    client=self.client,
                ),
            )

        else:
            if self.dsn is None:
                raise exc.internal(
                    "dsn is required for PostgresLifecycleModule when client is not routed"
                )

            steps.append(
                postgres_lifecycle_step(
                    self.pool_step_name,
                    dsn=self.dsn,
                    config=self.config,
                ),
            )

        if self.searches or self.hub_searches or self.federated_searches:
            steps.append(
                postgres_catalog_warmup_lifecycle_step(
                    self.warmup_step_name,
                    searches=self.searches,
                    hub_searches=self.hub_searches,
                    federated_searches=self.federated_searches,
                ),
            )

        if self.schema_specs:
            steps.append(
                postgres_document_schema_validation_lifecycle_step(
                    self.schema_step_name,
                    specs=self.schema_specs,
                ),
            )

        return tuple(steps)
