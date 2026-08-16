# Transactional notifications

Emitting a notification that must not fire if the transaction rolls back: stage inside the transaction, relay after commit. One procedure, and the shape is load-bearing.

## Transactional notifications

```python
from forze.application.contracts.outbox import OutboxDestination, OutboxSpec
from forze_kits.integrations.notify import (
    EmailNotification,
    NotificationRouter,
    process_notification_message,
)
from forze_kits.integrations.outbox import EmitMapping, OutboxEmit, bind_outbox

events_spec = OutboxSpec(
    name="events",
    codec=...,
    destination=OutboxDestination.queue(route="notifications", channel="notifications"),
)

# One declaration binds all three pieces: the domain-event -> outbox staging bridge,
# the in-tx flush hook, and the background relay lifecycle step.
wiring = bind_outbox(
    OutboxEmit(
        spec=events_spec,
        emits=(EmitMapping(event=ProjectCreated, event_type="project.created", to_payload=...),),
    )
)

router = NotificationRouter()
router.register("project.created", lambda e: [EmailNotification(...)])
# merge wiring.domain_event_registry() into the deps module; the worker calls
# process_notification_message on each relayed message
```

`bind_outbox` returns an `OutboxWiring` carrying the staging bridges (merge into a `DomainEventRegistry`), the in-tx flush hook (attach to the write operation's plan), and — when `OutboxEmit.relay` is set — the background relay lifecycle step. The backend `outboxes={name: cfg}` config stays in the deps module.

See [Transactional notifications](https://morzecrew.github.io/forze/latest/recipes/transactional-notifications/).

## Reference

- [Transactional notifications](https://morzecrew.github.io/forze/latest/recipes/transactional-notifications/)
