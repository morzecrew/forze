"""The required_reach floor on direct transports: a route weaker than the floor fails closed.

A transport has no ``at_rest`` level, so its only encrypted reach is ``end_to_end``; a
floor therefore admits a transport route only if it is ``end_to_end`` (or the floor is
``none``/unset).
"""

from __future__ import annotations

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.base import EncryptionReach, MessageEncryptionTier
from forze.application.contracts.crypto import RequiredReachDepKey
from forze.application.contracts.pubsub import PubSubCommandDepKey, PubSubSpec
from forze.application.contracts.queue import QueueCommandDepKey, QueueSpec
from forze.application.contracts.stream import StreamCommandDepKey, StreamSpec
from forze.application.execution import Deps, DepsRegistry, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from forze_mock import MockDepsModule

# ----------------------- #


class _Msg(BaseModel):
    n: int


@attrs.define(slots=True, frozen=True, kw_only=True)
class _ReachFloorModule:
    reach: EncryptionReach

    def __call__(self) -> Deps:
        return Deps.plain({RequiredReachDepKey: self.reach})


def _runtime(*, floor: EncryptionReach | None) -> ExecutionRuntime:
    modules = [MockDepsModule()]
    if floor is not None:
        modules.append(_ReachFloorModule(reach=floor))
    return ExecutionRuntime(deps=DepsRegistry.from_modules(*modules).freeze())


_CODEC = PydanticModelCodec(_Msg)


# ....................... #


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "key", "spec_factory"),
    [
        ("queue", QueueCommandDepKey, lambda enc: QueueSpec(name="t", codec=_CODEC, encryption=enc)),
        ("pubsub", PubSubCommandDepKey, lambda enc: PubSubSpec(name="t", codec=_CODEC, encryption=enc)),
        ("stream", StreamCommandDepKey, lambda enc: StreamSpec(name="t", codec=_CODEC, encryption=enc)),
    ],
)
async def test_plaintext_transport_rejected_under_e2e_floor(kind, key, spec_factory) -> None:
    spec = spec_factory("none")
    runtime = _runtime(floor="end_to_end")
    async with runtime.scope():
        ctx = runtime.get_context()
        with pytest.raises(CoreException) as ei:
            ctx.deps.resolve_configurable(ctx, key, spec, route=spec.name)
        assert ei.value.code == f"core.{kind}.reach_floor"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("key", "spec_factory"),
    [
        (QueueCommandDepKey, lambda enc: QueueSpec(name="t", codec=_CODEC, encryption=enc)),
        (PubSubCommandDepKey, lambda enc: PubSubSpec(name="t", codec=_CODEC, encryption=enc)),
        (StreamCommandDepKey, lambda enc: StreamSpec(name="t", codec=_CODEC, encryption=enc)),
    ],
)
async def test_e2e_transport_satisfies_e2e_floor(key, spec_factory) -> None:
    spec = spec_factory("end_to_end")
    runtime = _runtime(floor="end_to_end")
    async with runtime.scope():
        ctx = runtime.get_context()
        assert ctx.deps.resolve_configurable(ctx, key, spec, route=spec.name) is not None


@pytest.mark.asyncio
async def test_no_floor_allows_plaintext_transport() -> None:
    enc: MessageEncryptionTier = "none"
    spec = QueueSpec(name="t", codec=_CODEC, encryption=enc)
    runtime = _runtime(floor=None)
    async with runtime.scope():
        ctx = runtime.get_context()
        assert (
            ctx.deps.resolve_configurable(ctx, QueueCommandDepKey, spec, route=spec.name)
            is not None
        )
