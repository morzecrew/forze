"""The required_reach floor: an outbox route weaker than the declared minimum fails closed."""

from __future__ import annotations

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.base import EncryptionReach
from forze.application.contracts.crypto import (
    KeyRef,
    RequiredReachDepKey,
    StaticKeyDirectory,
)
from forze.application.contracts.outbox import OutboxSpec
from forze.application.execution import (
    CryptoDepsModule,
    Deps,
    DepsRegistry,
    ExecutionRuntime,
)
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from forze_mock import MockDepsModule, MockKeyManagement

# ----------------------- #


class _EventPayload(BaseModel):
    n: int


@attrs.define(slots=True, frozen=True, kw_only=True)
class _ReachFloorModule:
    """Tiny deps module registering a deployment-wide required_reach floor."""

    reach: EncryptionReach

    def __call__(self) -> Deps:
        return Deps.plain({RequiredReachDepKey: self.reach})


def _spec(*, encryption: EncryptionReach) -> OutboxSpec[_EventPayload]:
    return OutboxSpec(
        name="events", codec=PydanticModelCodec(_EventPayload), encryption=encryption
    )


def _runtime(*, floor: EncryptionReach | None) -> ExecutionRuntime:
    modules = [MockDepsModule()]
    if floor is not None:
        modules.append(_ReachFloorModule(reach=floor))
    return ExecutionRuntime(deps=DepsRegistry.from_modules(*modules).freeze())


# ....................... #


def _crypto_module(reach: EncryptionReach | None) -> CryptoDepsModule:
    return CryptoDepsModule(
        kms=MockKeyManagement(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
        required_reach=reach,
    )


def test_crypto_deps_module_registers_required_reach() -> None:
    deps = _crypto_module("end_to_end")()
    assert deps.exists(RequiredReachDepKey)


def test_crypto_deps_module_no_floor_by_default() -> None:
    deps = _crypto_module(None)()
    assert not deps.exists(RequiredReachDepKey)


# ....................... #


@pytest.mark.asyncio
async def test_plaintext_route_rejected_under_e2e_floor() -> None:
    runtime = _runtime(floor="end_to_end")
    async with runtime.scope():
        ctx = runtime.get_context()
        with pytest.raises(CoreException) as ei:
            ctx.outbox.command(_spec(encryption="none"))
        assert ei.value.code == "core.outbox.reach_floor"


@pytest.mark.asyncio
async def test_at_rest_route_rejected_under_e2e_floor() -> None:
    runtime = _runtime(floor="end_to_end")
    async with runtime.scope():
        ctx = runtime.get_context()
        with pytest.raises(CoreException) as ei:
            ctx.outbox.command(_spec(encryption="at_rest"))
        assert ei.value.code == "core.outbox.reach_floor"


@pytest.mark.asyncio
async def test_e2e_route_satisfies_e2e_floor() -> None:
    runtime = _runtime(floor="end_to_end")
    async with runtime.scope():
        ctx = runtime.get_context()
        # Resolves without raising (e2e meets the floor; keyring comes from MockDepsModule).
        assert ctx.outbox.command(_spec(encryption="end_to_end")) is not None


@pytest.mark.asyncio
async def test_at_rest_route_satisfies_at_rest_floor() -> None:
    runtime = _runtime(floor="at_rest")
    async with runtime.scope():
        ctx = runtime.get_context()
        assert ctx.outbox.command(_spec(encryption="at_rest")) is not None


@pytest.mark.asyncio
async def test_no_floor_allows_plaintext_route() -> None:
    runtime = _runtime(floor=None)
    async with runtime.scope():
        ctx = runtime.get_context()
        assert ctx.outbox.command(_spec(encryption="none")) is not None
