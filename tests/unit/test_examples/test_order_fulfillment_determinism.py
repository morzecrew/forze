"""DST exit-gate: the end-to-end order-fulfillment flow is byte-deterministic.

Binds a ``FrozenTimeSource`` + ``SeededEntropySource`` and runs the full
aggregate → event → saga → outbox → relay → inbox → downstream flow twice. With
both seams bound, every id (uuid7 timestamp + random bits, downstream uuid4),
timestamp, and serialized payload is a pure function of (instant, seed), so the
two runs must produce byte-identical snapshots.

This is the representative-slice gate for DST phase P0. The broader P0 exit
criterion — the whole unit suite byte-identical across *processes* — additionally
requires ``PYTHONHASHSEED=0``; within this single process both runs share the same
hash seed, so the comparison is already fair. (No Argon2 here: this slice has no
password path, so the hash-output exclusion does not apply.)
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from examples.recipes.order_fulfillment.app import (
    INVENTORY_SPEC,
    ORDER_SPEC,
    SHIPMENT_SPEC,
    build_context,
    deliver,
    place_order,
    relay_once,
    run_checkout,
)
from forze.base.primitives import (
    FrozenTimeSource,
    SeededEntropySource,
    bind_entropy_source,
    bind_time_source,
)

# ----------------------- #

_T0 = datetime(2020, 1, 1, 12, 0, tzinfo=UTC)


async def _run(seed: int) -> bytes:
    """Run the full flow under bound seams; return a canonical byte snapshot."""

    with bind_time_source(FrozenTimeSource(instant=_T0)):
        with bind_entropy_source(SeededEntropySource(seed=seed)):
            ctx = build_context()

            order_id, inventory_id = await place_order(ctx)
            await run_checkout(ctx, order_id, inventory_id)
            messages = await relay_once(ctx)
            delivered = await deliver(ctx, messages[0])

            order = await ctx.document.query(ORDER_SPEC).get(order_id)
            inventory = await ctx.document.query(INVENTORY_SPEC).get(inventory_id)
            shipments = await ctx.document.query(SHIPMENT_SPEC).find_many()

            snapshot = {
                "order_id": str(order_id),
                "inventory_id": str(inventory_id),
                "delivered": delivered,
                "messages": [
                    {"id": m.id, "order_id": str(m.order_id)} for m in messages
                ],
                "order": order.model_dump(mode="json"),
                "inventory": inventory.model_dump(mode="json"),
                "shipments": [h.model_dump(mode="json") for h in shipments.hits],
            }

    return json.dumps(snapshot, sort_keys=True, default=str).encode("utf-8")


# ....................... #


class TestEndToEndDeterminism:
    async def test_same_seed_is_byte_identical(self) -> None:
        first = await _run(seed=1234)
        second = await _run(seed=1234)
        assert first == second

    async def test_ids_actually_flow_from_entropy(self) -> None:
        # Sanity: the determinism is the seam's doing, not a constant flow — a
        # different seed must perturb the generated ids/payloads.
        assert await _run(seed=1) != await _run(seed=2)
