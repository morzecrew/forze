"""Identity helpers shared by the durable stores.

Both stores read a tenant back out of a document. A caller-supplied id is scoped under it by
the contract's :func:`~forze.application.contracts.durable.function.scope_tenant_key`, the
one encoding every durable store shares.
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
