"""Transactional outbox contracts for integration events."""

from .deps import (
    OutboxCommandDepKey,
    OutboxCommandDepPort,
    OutboxDeps,
    OutboxQueryDepKey,
    OutboxQueryDepPort,
)
from .ports import OutboxCommandPort, OutboxQueryPort
from .specs import OutboxDestination, OutboxDestinationKind, OutboxSpec
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
    "IntegrationEvent",
    "OutboxClaim",
    "OutboxCommandDepKey",
    "OutboxCommandDepPort",
    "OutboxCommandPort",
    "OutboxDeps",
    "OutboxDestination",
    "OutboxDestinationKind",
    "OutboxQueryDepKey",
    "OutboxQueryDepPort",
    "OutboxQueryPort",
    "OutboxRelayResult",
    "OutboxSpec",
    "OutboxStagingContext",
    "OutboxStatus",
    "StagedOutboxEntry",
]
