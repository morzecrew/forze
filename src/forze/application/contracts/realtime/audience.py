"""Realtime audiences — the logical addressing vocabulary for server push.

An :class:`Audience` names *who* a realtime emit targets in **logical** terms:
a principal (an identity) or a topic (a free string-key selector for an
app-defined group). It is deliberately transport-neutral and carries **no
tenant** — the application is not aware that emits are tenant-scoped at all.
Tenant scoping, and the concrete wire/room representation, are applied below the
contract by the adapter from the bound invocation identity, exactly like every
other tenant-aware port (pubsub, cache, document).

There is intentionally no "whole tenant" audience: that the contract cannot name
a tenant is the point. An app that wants a tenant-wide broadcast models it as a
topic its connections all join — its own convention, invisible here.
"""

from enum import StrEnum
from typing import final

import attrs

# ----------------------- #


class AudienceKind(StrEnum):
    """Kind of realtime audience."""

    PRINCIPAL = "principal"
    """A specific principal's live connections (identity-addressed)."""

    TOPIC = "topic"
    """An app-defined group named by a free string key (a chat, a document, …)."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class Audience:
    """Logical target of a realtime emit, resolved to a transport target by the adapter.

    The pair ``(kind, name)`` is the whole identity — a structured selector, not
    a wire string. There is no tenant and no connection/session here by design:
    both are ambient concerns the transport owns.
    """

    kind: AudienceKind
    """The kind of audience."""

    name: str
    """The selector key within the kind (principal id, or topic name)."""

    # ....................... #

    @classmethod
    def principal(cls, principal_id: str) -> "Audience":
        """A specific principal's live connections."""

        return cls(kind=AudienceKind.PRINCIPAL, name=principal_id)

    # ....................... #

    @classmethod
    def topic(cls, name: str) -> "Audience":
        """An app-defined group identified by the free string key *name*."""

        return cls(kind=AudienceKind.TOPIC, name=name)
