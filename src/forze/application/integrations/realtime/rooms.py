"""Audience → room naming — the single place the scoping scheme exists.

Every transport that groups connections by audience (Socket.IO rooms, SSE hub
subscriptions, presence entries) derives the name here, so publish, membership,
and presence can never disagree on what scope a signal addresses.
"""

from uuid import UUID

from forze.application.contracts.realtime import Audience

# ----------------------- #

__all__ = [
    "room_for",
]


def room_for(audience: Audience, tenant: UUID | None) -> str:
    """Resolve a logical *audience* to a tenant-scoped room name.

    When a tenant is bound the room is prefixed ``t:<tenant>:`` so tenants cannot
    share a room.
    """

    base = f"{audience.kind.value}:{audience.name}"

    return f"t:{tenant}:{base}" if tenant is not None else base
