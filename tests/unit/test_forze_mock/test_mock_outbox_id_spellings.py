"""The mock outbox reads a row's id as the real backends do: any spelling of it, in a list that
may mix ``UUID`` and ``str``, and a malformed one is ``precondition``."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.serialization import PydanticModelCodec
from forze_mock import MockDepsModule
from tests.support.outbox_id_spellings import SPELLINGS, check_outbox_id_spellings


class _Payload(BaseModel):
    n: int


@pytest.mark.asyncio
@pytest.mark.parametrize("spell", SPELLINGS)
async def test_mock_outbox_ids_conform(spell: Any) -> None:
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        await check_outbox_id_spellings(
            runtime.get_context(),
            OutboxSpec(name="events", codec=PydanticModelCodec(_Payload)),
            _Payload(n=1),
            spell,
        )
