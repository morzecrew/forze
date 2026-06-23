"""Postgres procedures execution config."""

from datetime import timedelta
from typing import TYPE_CHECKING, Any

import attrs

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_optional_named_resource_spec,
)
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.application.integrations.tenancy_sql import unreferenced_param_keys
from forze.base.exceptions import exc

if TYPE_CHECKING:
    from forze.application.contracts.procedure import ProcedureSpec

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresProcedureConfig(TenantAwareIntegrationConfig):
    """Physical Postgres mapping for one :class:`~forze.application.contracts.procedure.ProcedureSpec` route.

    One spec = one procedure, so the config is flat (no operations map). When ``tenant_aware``
    (inherited), the adapter binds the current tenant id as the ``%(tenant)s`` parameter and fails
    closed if no tenant is bound; the registered SQL must reference that parameter (checked at
    wiring). At the namespace tier, set ``query_schema`` so the statement runs in the tenant's own
    schema.
    """

    sql: str
    """PostgreSQL statement with psycopg named placeholders ``%(field)s`` (``%(tenant)s`` is
    injected when tenant-aware). A function call, ``CALL``, set-based DML, or ``REFRESH``."""

    in_transaction: bool = True
    """Run inside a transaction (default). Set ``False`` for statements that cannot run in one
    (``REFRESH MATERIALIZED VIEW CONCURRENTLY``, some maintenance) — uses the autocommit path."""

    statement_timeout: timedelta | None = None
    """Optional ``SET LOCAL statement_timeout`` for long compute. Requires a transaction."""

    query_schema: NamedResourceSpec | None = attrs.field(
        default=None,
        converter=coerce_optional_named_resource_spec,
    )
    """Per-tenant query schema (namespace tier) — a static name or ``(tenant_id) -> str``
    resolver. When set, the statement runs in a transaction with ``SET LOCAL search_path`` to the
    resolved (per-tenant) schema, so an unqualified table resolves in the tenant's own schema."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.sql.strip():
            raise exc.internal("Procedure sql must be non-empty.")

        if not self.in_transaction and self.statement_timeout is not None:
            raise exc.configuration(
                "Procedure statement_timeout requires in_transaction=True "
                "(SET LOCAL needs a transaction).",
                code="procedures_autocommit_timeout",
            )

        if not self.in_transaction and self.query_schema is not None:
            raise exc.configuration(
                "Procedure query_schema requires in_transaction=True "
                "(SET LOCAL search_path needs a transaction).",
                code="procedures_autocommit_schema",
            )

    # ....................... #

    def validate_against_spec(self, spec: "ProcedureSpec[Any, Any]") -> None:
        """Ensure the integration config aligns with the kernel :class:`ProcedureSpec`."""

        if self.tenant_aware and unreferenced_param_keys(
            {str(spec.name): self.sql}, pattern=r"%\(tenant\)s"
        ):
            raise exc.configuration(
                f"Postgres procedure route {str(spec.name)!r} is tenant_aware but its SQL never "
                "references the tenant parameter (%(tenant)s). A tenant-aware procedure must "
                "scope itself on the bound tenant.",
                code="procedures_tenant_param_unreferenced",
                details={"route": str(spec.name)},
            )
