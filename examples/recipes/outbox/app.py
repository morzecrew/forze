"""Recipe: transactional outbox — stage an integration event with the write, relay to a queue.

The event is staged in the SAME transaction as the business write, so publishing
can't happen unless the write commits (and vice versa). A relay then moves staged
rows to the broker. Mock-runnable — no broker needed.

Run it:  uv run python -m examples.recipes.outbox.app
Exercised by tests/unit/test_examples/test_outbox.py.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import structlog
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxDestination, OutboxSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.base.logging import configure_logging
from forze.base.logging.constants import LogLevel
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import OutboxRelay
from forze_mock import MockDepsModule

_LOGGER_NAME = "outbox"
log = structlog.get_logger(_LOGGER_NAME)


def _setup_logging(level: LogLevel) -> None:
    # Render this example's narration and any framework logs cleanly (and filter trace/debug),
    # **only when run as a script** — leaving global logging untouched so imports/tests are unaffected.
    configure_logging(level=level, logger_names=[_LOGGER_NAME, "forze"])


# --8<-- [start:event]
class OrderPlaced(BaseModel):
    order_id: str


# The outbox spec names its destination queue; its route must equal the queue's name.
ORDER_EVENTS = OutboxSpec(
    name="order-events",
    codec=PydanticModelCodec(OrderPlaced),
    destination=OutboxDestination.queue(route="orders", channel="orders"),
)
ORDERS_QUEUE = QueueSpec(name="orders", codec=PydanticModelCodec(OrderPlaced))
# --8<-- [end:event]


# --8<-- [start:stage]
async def place_order(ctx: ExecutionContext, order_id: str) -> None:
    # Your business write goes here, in a transaction. Stage the integration
    # event in the same unit of work, then flush — it commits with the write.
    outbox = ctx.outbox.command(ORDER_EVENTS)
    await outbox.stage("order.placed", OrderPlaced(order_id=order_id), event_id=uuid4())
    await outbox.flush()


# --8<-- [end:stage]


# --8<-- [start:relay]
async def relay(ctx: ExecutionContext) -> int:
    # In production this runs in the background (outbox_relay_background_lifecycle_step);
    # here we drive one pass. It claims staged rows and publishes them to the queue.
    result = await OutboxRelay(outbox_spec=ORDER_EVENTS).to_queue(ctx, ORDERS_QUEUE)
    return result.published


# --8<-- [end:relay]


async def main() -> None:
    ctx = ExecutionContext(
        deps=DepsRegistry.from_modules(MockDepsModule()).freeze().resolve()
    )
    await place_order(ctx, str(uuid4()))
    published = await relay(ctx)
    log.info("published events to the orders queue", published=published)


if __name__ == "__main__":
    _setup_logging("info")
    asyncio.run(main())
