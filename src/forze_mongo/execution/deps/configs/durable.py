"""Mongo durable-execution integration configuration."""

from typing import final

import attrs

from forze.application.contracts.resolution import RelationSpec, coerce_relation_spec
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoDurableStepConfig(TenantAwareIntegrationConfig):
    """Configuration for the Mongo durable-function step-memo journal.

    See :class:`~forze_mongo.adapters.durable.function_step.MongoDurableFunctionStepAdapter`.
    """

    collection: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Database and collection for the step journal (``run_id``, ``step_id``, ``result``, …)."""

    encrypt: bool = False
    """Seal journaled step results at rest under the wired keyring.

    When ``True`` the factory fails closed at resolve if no keyring is registered.
    """


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoDurableRunConfig(TenantAwareIntegrationConfig):
    """Configuration for the Mongo durable-run store.

    See :class:`~forze_mongo.adapters.durable.run_store.MongoDurableRunStore`.
    """

    collection: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Database and collection for run instances (one collection; tenant is a field)."""

    encrypt: bool = False
    """Seal journaled run input/output at rest under the wired keyring.

    When ``True`` the factory fails closed at resolve if no keyring is registered.
    """

    admin: bool = False
    """Also expose the :class:`DurableRunAdminPort` (ops control plane) over this collection.

    Opt-in so a deployment publishes the ops plane explicitly. **Not read-only**: the port
    carries ``request_cancel``, which lands a ``PENDING`` run in ``CANCELLED`` and stamps a
    ``RUNNING`` one for its holder to stop. Enabling it grants run *control*, not just
    visibility — the ask is tenant-scoped and unfenced, so anyone who can resolve this key
    can stop any run their tenant can list.
    """


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoDurableScheduleConfig(TenantAwareIntegrationConfig):
    """Configuration for the Mongo durable-schedule store.

    See :class:`~forze_mongo.adapters.durable.schedule_store.MongoDurableScheduleStore`.
    """

    collection: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Database and collection for recurring schedules (one collection; tenant is a field)."""
