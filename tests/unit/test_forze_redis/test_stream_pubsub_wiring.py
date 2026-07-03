"""RedisDepsModule wires the generic stream/pub-sub transports.

Resolving a configurable only *builds* the adapter (no Redis I/O), so these run without a
broker: they prove encryption wrapping, fail-closed keyring, the reach floor, at_rest
rejection, and the group query/admin plane split.
"""

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
from forze.application.contracts.pubsub import (
    PubSubCommandDepKey,
    PubSubQueryDepKey,
    PubSubSpec,
)
from forze.application.contracts.stream import (
    StreamCommandDepKey,
    AckStreamGroupAdminDepKey,
    AckStreamGroupQueryDepKey,
    StreamQueryDepKey,
    StreamSpec,
)
from forze.application.execution import (
    CryptoDepsModule,
    Deps,
    DepsRegistry,
    ExecutionRuntime,
)
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from forze_mock import MockKeyManagement
from forze_redis import (
    RedisDepsModule,
    RedisPubSubConfig,
    RedisStreamConfig,
    RedisStreamGroupConfig,
)
from forze_redis.adapters import (
    RedisPubSubAdapter,
    RedisStreamAdapter,
    RedisStreamGroupAdapter,
    RedisStreamGroupAdminAdapter,
)
from forze_redis.kernel.client import RedisClient

# ----------------------- #


class _Msg(BaseModel):
    n: int


_CODEC = PydanticModelCodec(_Msg)


@attrs.define(slots=True, frozen=True, kw_only=True)
class _ReachFloorModule:
    reach: EncryptionReach

    def __call__(self) -> Deps:
        return Deps.plain({RequiredReachDepKey: self.reach})


def _runtime(*, keyring: bool = False, floor: EncryptionReach | None = None) -> ExecutionRuntime:
    modules: list[object] = [
        RedisDepsModule(
            client=RedisClient(),
            streams={"s": RedisStreamConfig(tenant_aware=False)},
            stream_groups={"s": RedisStreamGroupConfig(tenant_aware=False)},
            pubsub={"p": RedisPubSubConfig(tenant_aware=False)},
        )
    ]
    if keyring:
        modules.append(
            CryptoDepsModule(
                kms=MockKeyManagement(), directory=StaticKeyDirectory(KeyRef(key_id="cmk"))
            )
        )
    if floor is not None:
        modules.append(_ReachFloorModule(reach=floor))
    return ExecutionRuntime(deps=DepsRegistry.from_modules(*modules).freeze())  # type: ignore[arg-type]


def _stream(enc: str = "none") -> StreamSpec[_Msg]:
    return StreamSpec(name="s", codec=_CODEC, encryption=enc)  # type: ignore[arg-type]


def _pubsub(enc: str = "none") -> PubSubSpec[_Msg]:
    return PubSubSpec(name="p", codec=_CODEC, encryption=enc)  # type: ignore[arg-type]


# ....................... #


@pytest.mark.asyncio
async def test_stream_query_returns_raw_adapter() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        port = ctx.deps.resolve_configurable(ctx, StreamQueryDepKey, _stream(), route="s")
        assert isinstance(port, RedisStreamAdapter)


@pytest.mark.asyncio
async def test_stream_command_plaintext_is_unwrapped() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        port = ctx.deps.resolve_configurable(ctx, StreamCommandDepKey, _stream(), route="s")
        assert isinstance(port, RedisStreamAdapter)  # no encryption → raw adapter


@pytest.mark.asyncio
async def test_stream_command_encrypted_is_wrapped_with_keyring() -> None:
    runtime = _runtime(keyring=True)
    async with runtime.scope():
        ctx = runtime.get_context()
        port = ctx.deps.resolve_configurable(
            ctx, StreamCommandDepKey, _stream("end_to_end"), route="s"
        )
        assert not isinstance(port, RedisStreamAdapter)  # wrapped in EncryptingStreamCommand


@pytest.mark.asyncio
async def test_stream_command_encrypted_without_keyring_fails_closed() -> None:
    runtime = _runtime(keyring=False)
    async with runtime.scope():
        ctx = runtime.get_context()
        with pytest.raises(CoreException, match="keyring"):
            ctx.deps.resolve_configurable(
                ctx, StreamCommandDepKey, _stream("end_to_end"), route="s"
            )


@pytest.mark.asyncio
async def test_stream_command_rejected_under_e2e_floor() -> None:
    runtime = _runtime(floor="end_to_end")
    async with runtime.scope():
        ctx = runtime.get_context()
        with pytest.raises(CoreException) as ei:
            ctx.deps.resolve_configurable(ctx, StreamCommandDepKey, _stream("none"), route="s")
        assert ei.value.code == "core.stream.reach_floor"


@pytest.mark.asyncio
async def test_stream_command_at_rest_rejected() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with pytest.raises(CoreException) as ei:
            ctx.deps.resolve_configurable(
                ctx, StreamCommandDepKey, _stream("at_rest"), route="s"
            )
        assert ei.value.code == "core.stream.invalid_reach"


@pytest.mark.asyncio
async def test_stream_group_query_admin_split() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        query = ctx.deps.resolve_configurable(
            ctx, AckStreamGroupQueryDepKey, _stream(), route="s"
        )
        admin = ctx.deps.resolve_configurable(
            ctx, AckStreamGroupAdminDepKey, _stream(), route="s"
        )
        assert isinstance(query, RedisStreamGroupAdapter)
        assert isinstance(admin, RedisStreamGroupAdminAdapter)
        # The data-plane query reference cannot reach group provisioning.
        assert not hasattr(query, "ensure_group")
        assert hasattr(admin, "ensure_group")


@pytest.mark.asyncio
@pytest.mark.parametrize("key", [StreamQueryDepKey, AckStreamGroupQueryDepKey])
async def test_stream_read_rejected_under_e2e_floor(key) -> None:
    # The floor gates reads too, not just publishes (defense-in-depth, mirrors the mock).
    runtime = _runtime(floor="end_to_end")
    async with runtime.scope():
        ctx = runtime.get_context()
        with pytest.raises(CoreException) as ei:
            ctx.deps.resolve_configurable(ctx, key, _stream("none"), route="s")
        assert ei.value.code == "core.stream.reach_floor"


@pytest.mark.asyncio
@pytest.mark.parametrize("key", [StreamQueryDepKey, AckStreamGroupQueryDepKey])
async def test_stream_read_at_rest_rejected(key) -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with pytest.raises(CoreException) as ei:
            ctx.deps.resolve_configurable(ctx, key, _stream("at_rest"), route="s")
        assert ei.value.code == "core.stream.invalid_reach"


@pytest.mark.asyncio
async def test_pubsub_subscribe_rejected_under_e2e_floor() -> None:
    runtime = _runtime(floor="end_to_end")
    async with runtime.scope():
        ctx = runtime.get_context()
        with pytest.raises(CoreException) as ei:
            ctx.deps.resolve_configurable(ctx, PubSubQueryDepKey, _pubsub("none"), route="p")
        assert ei.value.code == "core.pubsub.reach_floor"


@pytest.mark.asyncio
async def test_pubsub_command_encryption_and_plaintext() -> None:
    runtime = _runtime(keyring=True)
    async with runtime.scope():
        ctx = runtime.get_context()
        plain = ctx.deps.resolve_configurable(ctx, PubSubCommandDepKey, _pubsub(), route="p")
        sealed = ctx.deps.resolve_configurable(
            ctx, PubSubCommandDepKey, _pubsub("end_to_end"), route="p"
        )
        assert isinstance(plain, RedisPubSubAdapter)
        assert not isinstance(sealed, RedisPubSubAdapter)
