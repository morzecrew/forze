"""Postgres durable-execution integration configuration."""

from typing import final

import attrs

from forze.application.contracts.resolution import RelationSpec, coerce_relation_spec
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresDurableStepConfig(TenantAwareIntegrationConfig):
    """Configuration for the Postgres durable-function step-memo journal.

    See :class:`~forze_postgres.adapters.durable.function_step.PostgresDurableFunctionStepAdapter`.
    """

    relation: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Schema-qualified ``durable_step`` table (``run_id``, ``step_id``, ``result``, …)."""

    encrypt: bool = False
    """Seal journaled step results at rest under the wired keyring.

    When ``True`` the factory fails closed at resolve if no keyring is registered.
    """


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresDurableRunConfig(TenantAwareIntegrationConfig):
    """Configuration for the Postgres durable-run store.

    See :class:`~forze_postgres.adapters.durable.run_store.PostgresDurableRunStore`.
    """

    relation: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Schema-qualified ``durable_run`` table (single relation; tenant is a column)."""

    encrypt: bool = False
    """Seal journaled run input/output at rest under the wired keyring.

    When ``True`` the factory fails closed at resolve if no keyring is registered.
    """

    admin: bool = False
    """Also expose the :class:`DurableRunAdminPort` (ops control plane) over this table.

    Opt-in so a deployment publishes the ops plane explicitly. When ``True`` the module
    registers ``DurableRunAdminDepKey`` alongside the run store — a CQRS ``QUERY`` handler can
    ``list_runs`` without acquiring the claim/write store.

    **Not read-only.** The port also carries ``request_cancel``, which mutates run state: it
    lands a ``PENDING`` run in ``CANCELLED`` outright and stamps a ``RUNNING`` one for its
    holder to stop. Enabling this therefore grants run *control*, not just visibility — the
    ask is tenant-scoped and unfenced, so anyone who can resolve this key can stop any run
    their tenant can list. Wire it where that is intended.
    """


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresDurableScheduleConfig(TenantAwareIntegrationConfig):
    """Configuration for the Postgres durable-schedule store.

    See :class:`~forze_postgres.adapters.durable.schedule_store.PostgresDurableScheduleStore`.
    """

    relation: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Schema-qualified ``durable_schedule`` table (single relation; tenant is a column)."""
