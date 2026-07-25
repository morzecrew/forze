"""Notification fan-out — pub/sub as a change-source adapter.

Two halves:

- **Publisher side** (rotator / control plane): ``SecretRotated`` is staged
  **through the outbox** after a rotation's finish step commits, then relayed onto a
  broadcast pub/sub channel. Outbox gives the reliability; pub/sub gives the
  fan-out. The at-least-once guarantee ends at the broker — pub/sub is live-only —
  which is fine here: a missed message is covered by the ``fingerprint_ttl`` floor.

- **Subscriber side** (every app container): :class:`PubSubSecretsChangeSource`
  presents received events as :class:`~forze.application.contracts.secrets.SecretChanged`
  through the standard source seam. The binder can't tell it apart from a poll
  watcher.

Delivery semantics: at-least-once, unordered, duplicate-tolerant, advisory. A
duplicate or replay costs one no-op eviction; consumers re-resolve values through
their own authenticated store connection, so the channel is non-sensitive by
construction and needs no dedup, ordering keys, or persistence beyond the outbox's.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Collection
from typing import Final, final

import attrs

from forze.application.contracts.outbox import OutboxDestination, OutboxSpec
from forze.application.contracts.pubsub import PubSubQueryPort, PubSubSpec
from forze.application.contracts.secrets import (
    SECRET_ROTATED_EVENT_TYPE,
    SecretChanged,
    SecretRef,
    SecretRotated,
    SecretsChangeSource,
    SecretVersion,
)
from forze.application.execution.context import ExecutionContext
from forze.base.primitives import StrKey
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations._logger import logger

# ----------------------- #

DEFAULT_SECRET_ROTATIONS_CHANNEL: Final[str] = "secrets.rotations"
"""Broadcast pub/sub channel carrying :class:`SecretRotated` events. Operator-internal
(containers of one deployment), consistent with existing operational channels."""


def secret_rotated_outbox_spec(
    *,
    name: StrKey = "secret-rotations",
    route: StrKey = "secret-rotations",
    channel: str = DEFAULT_SECRET_ROTATIONS_CHANNEL,
) -> OutboxSpec[SecretRotated]:
    """Outbox route the rotator stages :class:`SecretRotated` on, destined for pub/sub."""

    return OutboxSpec(
        name=name,
        codec=PydanticModelCodec(SecretRotated),
        destination=OutboxDestination.pubsub(route=route, channel=channel),
    )


def secret_rotated_pubsub_spec(*, name: StrKey = "secret-rotations") -> PubSubSpec[SecretRotated]:
    """Pub/sub spec the subscriber side resolves its query port with.

    Use the same *name* as the outbox spec's destination route so the relay and the
    subscriber land on one transport binding.
    """

    return PubSubSpec(name=name, codec=PydanticModelCodec(SecretRotated))


# ....................... #


async def publish_secret_rotated(
    ctx: ExecutionContext,
    spec: OutboxSpec[SecretRotated],
    event: SecretRotated,
) -> None:
    """Stage and flush one rotation notification through the outbox.

    Call after the rotation's finish step has promoted the new value; the relay
    (``outbox_relay_background_lifecycle_step`` with a pubsub transport) carries it
    to the broadcast channel.
    """

    outbox = ctx.outbox.command(spec)
    await outbox.stage(SECRET_ROTATED_EVENT_TYPE, event)
    await outbox.flush()


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class PubSubSecretsChangeSource(SecretsChangeSource):
    """Presents a broadcast rotation channel as a standard change source.

    Wire it into the hot-reload binder exactly like the poll watcher; reconnects are
    the transport client's concern (the Redis backend auto-reconnects) plus the
    binder's supervised restart around a closed subscription.
    """

    query: PubSubQueryPort[SecretRotated]
    """Resolved pub/sub query port (route = the rotation spec's name)."""

    channel: str = DEFAULT_SECRET_ROTATIONS_CHANNEL
    """Broadcast channel to subscribe."""

    # ....................... #

    async def subscribe(
        self,
        refs: Collection[SecretRef] | None = None,
    ) -> AsyncIterator[SecretChanged]:
        """Yield rotation events as changes, filtered to *refs* when given."""

        paths = None if refs is None else frozenset(ref.path for ref in refs)

        async for message in self.query.subscribe([self.channel]):
            event = message.payload

            if not isinstance(event, SecretRotated):  # pyright: ignore[reportUnnecessaryIsInstance]
                # A foreign payload on the channel is advisory noise, never an error:
                # a consumer re-resolves through its own store, so nothing can be injected.
                logger.warning(
                    "Ignoring non-rotation payload on channel %s",
                    self.channel,
                )
                continue

            if paths is not None and event.ref_path not in paths:
                continue

            yield SecretChanged(
                ref=SecretRef(event.ref_path),
                version=SecretVersion(event.version_token),
            )
