"""Domain-event dispatch contracts."""

from .deps import (
    DomainDeps,
    DomainEventDispatcherDepKey,
    DomainEventDispatcherDepPort,
)
from .ports import DomainEventDispatcherPort, drain_domain_events

# ----------------------- #

__all__ = [
    "DomainDeps",
    "DomainEventDispatcherDepKey",
    "DomainEventDispatcherDepPort",
    "DomainEventDispatcherPort",
    "drain_domain_events",
]
