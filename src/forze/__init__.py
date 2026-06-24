"""Domain-Driven Design and Hexagonal Architecture for backend services.

This module is a curated **front door**: the handful of names you reach for to define
and run an aggregate, re-exported at the top level so a newcomer can write
``from forze import DocumentSpec, Document, build_runtime`` instead of memorising deep
paths. Everything else stays reachable at its full path (``forze.application.execution``,
``forze.application.contracts.document``, ``forze.domain.models``, …).

Pre-built CRUD wiring — facades, registries, DTOs — lives in the separate ``forze_kits``
package: ``forze`` is the contracts-and-runtime core and deliberately never imports it.

Re-exports resolve lazily (PEP 562), so ``import forze`` stays cheap and the execution
kernel is pulled in only when you actually touch one of these symbols.
"""

from typing import TYPE_CHECKING

from forze.base.lazy import lazy_exports

# ----------------------- #

# Curated name -> canonical module. The single source of truth for the front door;
# `__all__` and the lazy resolver below are derived from it.
_EXPORTS: dict[str, str] = {
    # runtime & wiring
    "build_runtime": "forze.application.execution",
    "ExecutionRuntime": "forze.application.execution",
    "ExecutionContext": "forze.application.execution",
    "DepsRegistry": "forze.application.execution",
    "Deps": "forze.application.execution",
    "DepsModule": "forze.application.execution",
    # read/write specs
    "DocumentSpec": "forze.application.contracts.document",
    "DocumentWriteTypes": "forze.application.contracts.document",
    # domain models
    "Document": "forze.domain.models",
    "ReadDocument": "forze.domain.models",
    "CreateDocumentCmd": "forze.domain.models",
    "BaseDTO": "forze.domain.models",
    "CoreModel": "forze.domain.models",
    "AggregateRoot": "forze.domain.models",
    "DomainEvent": "forze.domain.models",
    "event_emitter": "forze.domain.models",
    "invariant": "forze.domain.models",
}

__all__ = [
    "build_runtime",
    "ExecutionRuntime",
    "ExecutionContext",
    "DepsRegistry",
    "Deps",
    "DepsModule",
    "DocumentSpec",
    "DocumentWriteTypes",
    "Document",
    "ReadDocument",
    "CreateDocumentCmd",
    "BaseDTO",
    "CoreModel",
    "AggregateRoot",
    "DomainEvent",
    "event_emitter",
    "invariant",
]


__getattr__, __dir__ = lazy_exports(__name__, _EXPORTS)


if TYPE_CHECKING:
    # Eager imports for IDEs and type checkers only
    from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
    from forze.application.execution import (
        Deps,
        DepsModule,
        DepsRegistry,
        ExecutionContext,
        ExecutionRuntime,
        build_runtime,
    )
    from forze.domain.models import (
        AggregateRoot,
        BaseDTO,
        CoreModel,
        CreateDocumentCmd,
        Document,
        DomainEvent,
        ReadDocument,
        event_emitter,
        invariant,
    )
