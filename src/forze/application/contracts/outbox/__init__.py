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
from .ports import OutboxCommandPort, OutboxQueryPort, OutboxRowPersistPort
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
    OutboxRelayResult,
    OutboxStatus,
    StagedOutboxEntry,
)

# ----------------------- #

__all__ = [
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
    "StagedOutboxEntry",
]
