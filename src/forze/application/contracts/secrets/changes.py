"""The secrets change feed — a separate contract, deliberately not realtime/messaging.

A secrets change is an *infrastructure* signal between containers: it has no
audiences, no mailboxes, no client cursors, no egress tiers. Reusing the realtime or
messaging contracts would couple secret rotation to those planes' evolution; the
pub/sub *transport* may still carry these events, but as an adapter behind this seam,
never as the seam.

Delivery is at-least-once, unordered, and advisory: consumers must be idempotent —
which the eviction path already is (an eviction on an unchanged secret re-resolves,
recomputes an equal fingerprint, and rebuilds nothing). Signals accelerate; the
``fingerprint_ttl`` floor guarantees.
"""

from collections.abc import AsyncIterator, Collection
from datetime import datetime
from typing import Final, Protocol, final

import attrs
from pydantic import BaseModel

from .value_objects import SecretRef
from .versioning import SecretVersion

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SecretChanged:
    """One observed change at one secret ref. Carries the ref and version — never the value."""

    ref: SecretRef
    """The ref whose value changed."""

    version: SecretVersion
    """The version now current. Never the value. A consumer that needs the payload
    re-resolves through :class:`~forze.application.contracts.secrets.SecretsPort` —
    which also makes a spoofed or replayed event harmless: it can trigger a refetch,
    never inject a value."""


# ....................... #


class SecretsChangeSource(Protocol):
    """One seam for poll and push change detection.

    Sources are lifecycle-owned (started and stopped as lifecycle steps or supervised
    loops), multi-subscriber, and advisory: delivery is at-least-once, unordered, and
    lossy-with-a-floor — a missed event is covered by the ``fingerprint_ttl`` polling
    floor, never by source-level persistence. Subscribers must be idempotent.
    """

    def subscribe(
        self,
        refs: Collection[SecretRef] | None = None,
    ) -> AsyncIterator[SecretChanged]:
        """Yield changes for *refs* (``None`` = everything the source covers).

        :param refs: Optional filter; sources ignore refs they do not cover.
        :returns: An async iterator of observed changes, ending when the source stops.
        """

        ...  # pragma: no cover


# ....................... #

SECRET_ROTATED_EVENT_TYPE: Final[str] = "secrets.rotated"
"""Outbox/pub-sub event type for :class:`SecretRotated` notifications."""


@final
class SecretRotated(BaseModel):
    """Cross-container rotation notification payload — refs and versions only.

    Published through the outbox after a rotation's finish step commits, carried by
    broadcast pub/sub, consumed as a :class:`SecretChanged` by subscribers. The
    channel is non-sensitive by construction: values never transit.
    """

    ref_path: str
    """Path of the rotated secret's primary ref."""

    version_token: str
    """The version now current at the primary ref."""

    rotated_at: datetime
    """When the rotation's finish step promoted the new value."""
