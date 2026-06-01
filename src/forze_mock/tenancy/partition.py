"""Tenant-scoped key partitioning for mock adapters."""

from __future__ import annotations

from uuid import UUID

# ----------------------- #


def partition_namespace(tenant_id: UUID | None, namespace: str) -> str:
    """Prefix *namespace* with *tenant_id* when present.

    ``None`` tenant returns *namespace* unchanged (backward compatible).
    """

    if tenant_id is None:
        return namespace

    return f"{tenant_id}/{namespace}"
