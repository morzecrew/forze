"""Mock integration route configuration."""

from __future__ import annotations

import attrs

from forze.application.contracts.resolution import NamedResourceSpec, RelationSpec

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MockRouteConfig:
    """Per-route mock adapter options."""

    tenant_aware: bool = False
    """When ``True``, partition storage and apply row-level tenant filters."""

    namespace: NamedResourceSpec | None = None
    """Optional namespace override (static or resolver)."""

    relation: RelationSpec | None = None
    """Optional relation override (schema/table style)."""
