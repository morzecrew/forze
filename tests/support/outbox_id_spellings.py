"""One case every outbox backend runs: a row's id names it whether passed as a ``UUID`` or as
any text spelling of one, in a list that may mix the two. At every call that takes ids, a
malformed id is ``precondition`` and the call changes no row, and an empty list changes none."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxSpec
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow

SPELLINGS = [
    pytest.param(lambda pk: pk, id="uuid"),
    pytest.param(str, id="str"),
    pytest.param(lambda pk: pk.hex.upper(), id="upper-hex"),
]


async def check_outbox_id_spellings(
    ctx: ExecutionContext,
    spec: OutboxSpec[Any],
    payload: BaseModel,
    spell: Callable[[UUID], Any],
) -> None:
    """Stage and claim five rows, then mark, fail, retry and requeue them by *spell*'s ids."""

    command, query = ctx.outbox.command(spec), ctx.outbox.query(spec)
    admin = ctx.outbox.admin(spec)

    for _ in range(5):
        await command.stage("demo.created", payload)

    assert await command.flush() == 5

    claims = await query.claim_pending(limit=5)
    published, failed, retried, held, parked = [claim.id for claim in claims]
    other = uuid4()  # with a text spelling, each list mixes a UUID and text

    assert await query.mark_published([spell(published), other]) == 1
    assert await query.mark_failed([spell(failed), other], error="boom") == 1
    assert await query.mark_retry([spell(retried), other], attempts=1, available_at=utcnow()) == 1
    assert await query.requeue_failed([spell(failed), other]) == 1
    assert await query.mark_failed([parked]) == 1

    # Each call against a row it would act on: ``held`` is still processing, ``parked`` failed.
    calls: list[tuple[Callable[[list[Any]], Any], UUID]] = [
        (query.mark_published, held),
        (query.mark_failed, held),
        (lambda ids: query.mark_retry(ids, attempts=7, available_at=utcnow()), held),
        (query.requeue_failed, parked),
    ]
    depth = await admin.depth()

    for call, target in calls:
        assert await call([]) == 0

        with pytest.raises(CoreException) as refused:
            await call([spell(target), "not-a-uuid"])

        assert refused.value.kind == "precondition"
        # Refused whole: each call moves its row to another status, and none moved.
        assert await admin.depth() == depth

    assert await query.mark_published([held]) == 1
    assert await query.requeue_failed([parked]) == 1
