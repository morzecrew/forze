"""Transactional outbox contracts for integration events."""

from ..base import EncryptionReach
from .admin import OutboxAdminPort, OutboxDepth
from .deps import (
    OutboxAdminDepKey,
    OutboxAdminDepPort,
    OutboxCommandDepKey,
    OutboxCommandDepPort,
    OutboxDeps,
    OutboxQueryDepKey,
    OutboxQueryDepPort,
)
from .integration_config import OutboxIntegrationConfig
from .ports import (
    OutboxCommandPort,
    OutboxQueryPort,
    OutboxRowPersistPort,
    TransactionalOutboxPasses,
    outbox_pass_scope,
)
from .specs import (
    OutboxDestination,
    OutboxDestinationKind,
    OutboxEncryptionTier,
    OutboxSpec,
)
from .staging_context import OutboxStagingContext
from .value_objects import (
    IntegrationEvent,
    OutboxClaim,
    OutboxPassBudget,
    OutboxRelayResult,
    OutboxStatus,
    StagedOutboxEntry,
)
from .wake import DEFAULT_OUTBOX_WAKE_TOPIC, OutboxWake, OutboxWakeSignal

# ----------------------- #

__all__ = [
    "DEFAULT_OUTBOX_WAKE_TOPIC",
    "EncryptionReach",
    "IntegrationEvent",
    "OutboxAdminDepKey",
    "OutboxAdminDepPort",
    "OutboxAdminPort",
    "OutboxClaim",
    "OutboxCommandDepKey",
    "OutboxCommandDepPort",
    "OutboxCommandPort",
    "OutboxDepth",
    "OutboxDeps",
    "OutboxDestination",
    "OutboxIntegrationConfig",
    "OutboxPassBudget",
    "OutboxDestinationKind",
    "OutboxEncryptionTier",
    "OutboxQueryDepKey",
    "OutboxQueryDepPort",
    "OutboxQueryPort",
    "OutboxRelayResult",
    "OutboxRowPersistPort",
    "OutboxSpec",
    "OutboxStagingContext",
    "OutboxStatus",
    "OutboxWake",
    "OutboxWakeSignal",
    "StagedOutboxEntry",
    "TransactionalOutboxPasses",
    "outbox_pass_scope",
]
