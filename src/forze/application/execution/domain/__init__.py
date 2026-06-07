"""In-process domain-event dispatcher, registry, module, and outbox bridge."""

from .bridge import outbox_event_handler
from .dispatcher import InProcessDomainEventDispatcher
from .handler import DomainEventHandler, DomainEventHandlerFactory, DomainEventRegistry
from .module import DomainEventsDepsModule
from .resolve import domain_dispatcher_provider

# ----------------------- #

__all__ = [
    "DomainEventHandler",
    "DomainEventHandlerFactory",
    "DomainEventRegistry",
    "DomainEventsDepsModule",
    "InProcessDomainEventDispatcher",
    "domain_dispatcher_provider",
    "outbox_event_handler",
]
