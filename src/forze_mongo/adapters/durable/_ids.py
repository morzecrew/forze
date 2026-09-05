"""Identity helpers shared by the durable stores.

Both stores read a tenant back out of a document and both scope a caller-supplied id under
it, and the two halves have to agree exactly: a scope function whose unscope counterpart
strips a different prefix hands the caller an id it never registered.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

# ----------------------- #


def as_uuid(value: Any) -> UUID | None:
    """Read a tenant id back from a document, where it is stored as a string.

    ``None`` stays ``None`` — a single-tenant deployment writes no tenant, and the records
    it reads back carry none either.
    """

    if value is None:
        return None

    return value if isinstance(value, UUID) else UUID(str(value))


# ....................... #


def scope_id(value: str, tenant_id: UUID | None) -> str:
    """Namespace a caller-supplied id under its tenant.

    A shared **tagged** collection holds every tenant's documents under one key space, so
    two tenants registering the same schedule id — or a scheduler's
    ``{schedule_id}:{fire_epoch}`` idempotency key, which is the same string for all of
    them — would otherwise land on one document. Single-tenant ids (``tenant_id is None``)
    are stored verbatim, so a deployment that never had tenants keeps its on-disk shape.
    """

    return value if tenant_id is None else f"{tenant_id}:{value}"


# ....................... #


def unscope_id(stored: str, tenant_id: UUID | None) -> str:
    """Strip the prefix :func:`scope_id` added, so a record surfaces the caller's own id.

    The ``{uuid}:`` prefix is fixed-width, which is what makes the strip exact rather than a
    guess at where the caller's id starts.
    """

    if tenant_id is None:
        return stored

    prefix = f"{tenant_id}:"

    return stored[len(prefix) :] if stored.startswith(prefix) else stored
