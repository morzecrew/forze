"""Pre-built wiring above Forze contracts.

``forze_kits`` is the batteries-included layer: ready-made CRUD handlers, aggregate
facades, and DTOs assembled over the ``forze`` core contracts. A typical app imports from
**both** — the core for specs and the runtime, kits for the handlers and facade that drive
an aggregate. The split exists so the core never depends on these conveniences.

This is a curated front door for the most-used kit names; the rest stay reachable at their
full paths (``forze_kits.aggregates.document``, ``forze_kits.dto``, …). Re-exports resolve
lazily (PEP 562) so importing the package stays cheap.
"""

import importlib
from typing import TYPE_CHECKING, Any

# ----------------------- #

# Curated name -> canonical module (single source of truth for the front door).
_EXPORTS: dict[str, str] = {
    "DocumentFacade": "forze_kits.aggregates.document",
    "document_facade": "forze_kits.aggregates.document",
    "build_document_registry": "forze_kits.aggregates.document",
    "DocumentDTOs": "forze_kits.aggregates.document",
    "DocumentMappers": "forze_kits.aggregates.document",
    "DocumentKernelOp": "forze_kits.aggregates.document",
    "Paginated": "forze_kits.dto",
    "OutboxRelay": "forze_kits.integrations.outbox",
}

__all__ = [
    "DocumentFacade",
    "document_facade",
    "build_document_registry",
    "DocumentDTOs",
    "DocumentMappers",
    "DocumentKernelOp",
    "Paginated",
    "OutboxRelay",
]


def __getattr__(name: str) -> Any:
    """Resolve a curated export on first access (PEP 562 lazy import)."""

    module = _EXPORTS.get(name)

    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    return getattr(importlib.import_module(module), name)


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})


if TYPE_CHECKING:
    # Eager imports for IDEs and type checkers only.
    from forze_kits.aggregates.document import (
        DocumentDTOs,
        DocumentFacade,
        DocumentKernelOp,
        DocumentMappers,
        build_document_registry,
        document_facade,
    )
    from forze_kits.dto import Paginated
    from forze_kits.integrations.outbox import OutboxRelay
