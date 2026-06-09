"""Domain-event dispatch contracts."""

from .deps import (
    DomainDeps,
    DomainEventDispatcherDepKey,
    DomainEventDispatcherDepPort,
)
from .ports import DomainEventDispatcherPort

# ----------------------- #

__all__ = [
    "DomainDeps",
    "DomainEventDispatcherDepKey",
    "DomainEventDispatcherDepPort",
    "DomainEventDispatcherPort",
]
