"""Realtime audiences — the logical addressing vocabulary for server push.

An :class:`Audience` names *who* a realtime emit targets in **logical, ambient**
terms: a principal, a topic, or the whole current tenant. It is deliberately
transport-neutral and **tenant-agnostic** — the caller never names a tenant.
Tenant scoping is applied at the adapter boundary from the bound invocation
identity, exactly like every other tenant-aware port (pubsub, cache, document),
so a handler emitting to ``topic("chat")`` cannot reach another tenant's
``chat`` and never has to think about it.
"""

from enum import StrEnum
from typing import final

import attrs

# ----------------------- #


class AudienceKind(StrEnum):
    """Kind of realtime audience."""

    PRINCIPAL = "principal"
    """All of one principal's live connections."""

    TOPIC = "topic"
    """An app-defined channel (a chat, a document's collaborators)."""

    TENANT = "tenant"
    """Every connection in the current (ambient) tenant."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class Audience:
    """Logical target of a realtime emit, resolved to a transport target by the adapter.

    Build through the classmethods; the pair ``(kind, name)`` is the whole
    identity. There is no tenant and no connection/session here by design — both
    are ambient concerns the transport owns.
    """

    kind: AudienceKind
    """The kind of audience."""

    name: str = ""
    """Audience name within its kind (principal id, topic name); empty for ``tenant``."""

    # ....................... #

    @classmethod
    def principal(cls, principal_id: str) -> "Audience":
        """All of one principal's live connections."""

        return cls(kind=AudienceKind.PRINCIPAL, name=principal_id)

    # ....................... #

    @classmethod
    def topic(cls, name: str) -> "Audience":
        """An app-defined channel within the current tenant."""

        return cls(kind=AudienceKind.TOPIC, name=name)

    # ....................... #

    @classmethod
    def tenant(cls) -> "Audience":
        """Every connection in the current (ambient) tenant."""

        return cls(kind=AudienceKind.TENANT)

    # ....................... #

    def __str__(self) -> str:
        return f"{self.kind.value}:{self.name}" if self.name else self.kind.value
