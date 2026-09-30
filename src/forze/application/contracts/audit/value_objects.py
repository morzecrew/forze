"""Value objects for the audit contract."""

from collections.abc import Mapping
from datetime import datetime
from enum import StrEnum
from typing import final
from uuid import UUID

import attrs

# ----------------------- #

type AuditScalar = str | int | float | bool | None
"""A metadata value as it is stored: a JSON scalar. A ``UUID`` is accepted at the call and
stored as its string; anything else is refused (see
:meth:`~forze.application.contracts.audit.AuditSpec.check_metadata`)."""


@final
class AuditOutcome(StrEnum):
    """What happened to an audited operation."""

    ALLOWED = "allowed"
    """It ran and completed."""

    DENIED = "denied"
    """It was refused on access grounds — an authentication or authorization failure."""

    FAILED = "failed"
    """It was admitted and then failed."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuditObjectRef:
    """What an audited operation acted on."""

    type: str
    """The kind of object (``"snapshot"``, ``"interval"``)."""

    id: str
    """Its identifier."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuditEntry:
    """One audit row: who did what, on whose behalf, to what, and how it ended.

    ``actor_id`` is who performed the operation and ``subject_id`` on whose behalf it ran;
    they differ only for a delegated call. Both come from the invocation's authenticated
    identity, never from the operation's arguments.
    """

    action: str
    """The audited action, as its :class:`~forze.application.contracts.audit.AuditSpec` names it."""

    outcome: AuditOutcome
    """How the operation ended."""

    actor_id: UUID | None
    """The principal that performed it — the nearest actor of the chain when the call is
    delegated; ``None`` for an unauthenticated call."""

    subject_id: UUID | None
    """The principal it ran for; ``None`` for an unauthenticated call."""

    actor_ids: tuple[UUID, ...] = ()
    """The whole delegation chain, nearest actor first — ``actor_id`` is its first entry;
    empty for a direct or unauthenticated call. An agent acting through another is here too."""

    at: datetime
    """When it was recorded."""

    object_ref: AuditObjectRef | None = None
    """What it acted on, when the audited operation says."""

    metadata: Mapping[str, AuditScalar] = attrs.field(factory=dict)
    """The declared metadata, already checked against the spec's allowlist."""
