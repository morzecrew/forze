import pytest
from aio_pika import exceptions as aio_pika_errors

from forze.base.errors import CoreError, InfrastructureError
from forze_rabbitmq.kernel.platform.errors import rabbitmq_handled


@rabbitmq_handled("rabbitmq.test")
async def _raise(exc: Exception) -> None:
    raise exc


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("exc", "msg"),
    [
        (aio_pika_errors.AuthenticationError("auth"), "authentication failed"),
        (aio_pika_errors.AMQPConnectionError("conn"), "connection error"),
        (aio_pika_errors.IncompatibleProtocolError("proto"), "protocol mismatch"),
        (aio_pika_errors.ChannelInvalidStateError("state"), "invalid state"),
        (aio_pika_errors.AMQPChannelError("channel"), "channel error"),
        (TimeoutError("timeout"), "timed out"),
    ],
)
async def test_rabbitmq_error_handler_maps_known_exceptions(
    exc: Exception,
    msg: str,
) -> None:
    with pytest.raises(InfrastructureError, match=msg):
        await _raise(exc)


@pytest.mark.asyncio
async def test_rabbitmq_error_handler_passthrough_core_error() -> None:
    with pytest.raises(CoreError, match="already_core"):
        await _raise(CoreError("already_core"))


@pytest.mark.asyncio
async def test_rabbitmq_error_handler_maps_unknown_exception() -> None:
    with pytest.raises(InfrastructureError, match="RabbitMQ operation rabbitmq.test"):
        await _raise(RuntimeError("boom"))
