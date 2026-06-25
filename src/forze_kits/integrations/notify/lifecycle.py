"""Background lifecycle wiring for notification consumption."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from forze.application.contracts.execution import LifecycleStep
from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.queue import QueueSpec
from forze.base.primitives import StrKey

from .consumer import notification_queue_consumer_handler
from .events import integration_event_from_queue_message
from .routing import FrozenNotificationRouter
from .senders import NotificationSenders

# ----------------------- #


def notification_consumer_lifecycle_step(
    *,
    queue: str,
    queue_spec: QueueSpec[Any],
    inbox_spec: InboxSpec,
    tx_route: StrKey,
    router: FrozenNotificationRouter,
    senders: NotificationSenders,
    skip_unmapped: bool = True,
    bind_tenant_from_headers: bool = False,
    max_deliveries: int | None = None,
    retry_policy: StrKey | None = None,
    restart_backoff: timedelta = timedelta(seconds=5),
    step_id: StrKey | None = None,
) -> LifecycleStep:
    """Background lifecycle step that consumes a notification queue via ``QueueConsumer``.

    Notifications inherit the consumer's inbox dedup and poison parking. The dedup key is
    the same deterministic event id :func:`integration_event_from_queue_message` derives
    (the ``forze_event_id`` header the relay sets, else ``message.key``, else
    ``"<queue>:<message.id>"``), so an at-least-once redelivery cannot double-send.

    All ``QueueConsumer`` knobs (*max_deliveries*, *retry_policy*,
    *bind_tenant_from_headers*, ...) pass through with the same defaults and caveats.
    """

    # Local import: the consumer integration imports broadly; importing it at module load
    # would widen this module's import graph (and risk a cycle) for a rarely-hot path.
    from forze_kits.integrations.consumer import (
        queue_consumer_background_lifecycle_step,
    )

    return queue_consumer_background_lifecycle_step(
        queue=queue,
        queue_spec=queue_spec,
        handler=notification_queue_consumer_handler(
            router=router, senders=senders, skip_unmapped=skip_unmapped
        ),
        inbox_spec=inbox_spec,
        tx_route=tx_route,
        message_id=lambda m: str(integration_event_from_queue_message(m).event_id),
        bind_tenant_from_headers=bind_tenant_from_headers,
        max_deliveries=max_deliveries,
        retry_policy=retry_policy,
        restart_backoff=restart_backoff,
        step_id=step_id if step_id is not None else f"notification_consumer:{queue}",
    )
