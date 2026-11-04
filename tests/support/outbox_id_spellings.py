"""One case every outbox backend runs: a row's id names it whether passed as a ``UUID`` or as
any text spelling of one, in a list that may mix the two, and a malformed id is
``precondition`` at every call that takes ids."""

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
    """Stage and claim three rows, then mark, fail, retry and requeue them by *spell*'s ids."""

    command, query = ctx.outbox.command(spec), ctx.outbox.query(spec)

    for _ in range(3):
        await command.stage("demo.created", payload)

    assert await command.flush() == 3

    published, failed, retried = [claim.id for claim in await query.claim_pending(limit=3)]
    other = uuid4()  # with a text spelling, each list mixes a UUID and text

    assert await query.mark_published([spell(published), other]) == 1
    assert await query.mark_failed([spell(failed), other], error="boom") == 1
    assert await query.mark_retry([spell(retried), other], attempts=1, available_at=utcnow()) == 1
    assert await query.requeue_failed([spell(failed), other]) == 1

    calls: list[Callable[[list[Any]], Any]] = [
        query.mark_published,
        query.mark_failed,
        lambda ids: query.mark_retry(ids, attempts=1, available_at=utcnow()),
        query.requeue_failed,
    ]

    for call in calls:
        with pytest.raises(CoreException) as refused:
            await call([published, "not-a-uuid"])

        assert refused.value.kind == "precondition"
