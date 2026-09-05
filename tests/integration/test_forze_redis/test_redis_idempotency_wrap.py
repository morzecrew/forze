"""The idempotency wrap over the real Redis store — one invocation, begin and commit.

The fence lives in the store, but the owner comes from the ambient invocation, and the hook
resolves the port **twice** per operation: once for the middleware wrap that claims, once
for the commit. Redis is where that matters most, because its fence is a byte-exact
compare: if the two resolutions disagreed about the owner — a claim written carrying
``own`` and a commit sent without it, or the reverse — every successful operation would end
in a conflict instead of a stored record.

Nothing else in the suite would catch that. The store's own tests hand an owner in
directly, and the oracle's ownership check passes whenever *either* side is ownerless, so
the mock happily accepts a mismatch Redis rejects.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.idempotency import IdempotencySpec
from forze.application.execution.context.invocation import InvocationMetadata
from forze.application.hooks.idempotency import IdempotencyWrap
from forze.testing import context_from_modules
from forze_redis.execution.deps import RedisDepsModule
from forze_redis.execution.deps.configs import RedisIdempotencyConfig
from forze_redis.kernel.client import RedisClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_SPEC = IdempotencySpec(name="idem")


class _Args(BaseModel):
    n: int


class _Result(BaseModel):
    value: int


async def test_a_duplicate_replays_through_the_hook(redis_client: RedisClient) -> None:
    module = RedisDepsModule(
        client=redis_client,
        idempotency={"idem": RedisIdempotencyConfig(namespace=f"it:idem:{uuid4().hex[:12]}")},
    )
    ctx = context_from_modules(module)
    wrap = IdempotencyWrap(op="op", spec=_SPEC, result_type=_Result)(ctx)
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())
    calls = 0

    async def handler(args: _Args) -> _Result:
        nonlocal calls
        calls += 1
        return _Result(value=args.n)

    with (
        ctx.inv_ctx.bind_metadata(metadata=metadata),
        ctx.inv_ctx.bind_idempotency(f"k-{uuid4().hex[:8]}"),
    ):
        first = await wrap(handler, _Args(n=7))
        second = await wrap(handler, _Args(n=7))

    # The commit landed — a claim whose owner the commit could not match would have raised
    # instead — and the duplicate replayed it rather than running the handler again.
    assert calls == 1
    assert first.value == 7
    assert second.value == 7
