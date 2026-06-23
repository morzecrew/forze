"""Postgres procedures dep factory."""

from typing import TYPE_CHECKING, Any, final

import attrs

from forze.application.contracts.crypto import (
    DeterministicCipherDepKey,
    KeyringDepKey,
)
from forze.application.integrations.procedures import resolve_procedure_codecs_spec

from ....adapters.procedures import PostgresProceduresAdapter
from ..configs import PostgresProcedureConfig
from ..keys import PostgresClientDepKey

if TYPE_CHECKING:
    from forze.application.contracts.procedures import ProcedureSpec
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ConfigurablePostgresProcedures:
    """Build a :class:`PostgresProceduresAdapter` for a procedure spec route."""

    config: PostgresProcedureConfig
    """Postgres-specific configuration for the route."""

    # ....................... #

    def __call__(
        self,
        ctx: "ExecutionContext",
        spec: "ProcedureSpec[Any, Any]",
    ) -> PostgresProceduresAdapter[Any, Any]:
        self.config.validate_against_spec(spec)
        client = ctx.deps.provide(PostgresClientDepKey)
        spec = resolve_procedure_codecs_spec(
            spec,
            keyring=(
                ctx.deps.provide(KeyringDepKey)
                if ctx.deps.exists(KeyringDepKey)
                else None
            ),
            deterministic=(
                ctx.deps.provide(DeterministicCipherDepKey)
                if ctx.deps.exists(DeterministicCipherDepKey)
                else None
            ),
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
        return PostgresProceduresAdapter(
            client=client,
            spec=spec,
            config=self.config,
            tenant_provider=ctx.inv_ctx.get_tenant,
        )
