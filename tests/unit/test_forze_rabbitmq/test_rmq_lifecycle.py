from unittest.mock import AsyncMock, Mock

import pytest

from forze.application.execution import Deps
from forze_rabbitmq.execution.deps import RabbitMQClientDepKey
from forze_rabbitmq.execution.lifecycle import (
    RabbitMQShutdownHook,
    RabbitMQStartupHook,
    rabbitmq_lifecycle_step,
)
from forze_rabbitmq.kernel.client import RabbitMQClient, RabbitMQConfig
from tests.support.execution_context import (
    context_from_deps,
)


@pytest.mark.asyncio
async def test_rabbitmq_startup_hook_initializes_client() -> None:
    client = Mock(spec=RabbitMQClient)
    client.initialize = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({RabbitMQClientDepKey: client}))
    config = RabbitMQConfig(prefetch_count=10)
    hook = RabbitMQStartupHook(dsn="amqp://guest:guest@localhost/", config=config)

    await hook(ctx)

    from pydantic import SecretStr

    client.initialize.assert_awaited_once_with(
        SecretStr("amqp://guest:guest@localhost/"),
        config=config,
    )


@pytest.mark.asyncio
async def test_rabbitmq_shutdown_hook_closes_client() -> None:
    client = Mock(spec=RabbitMQClient)
    client.close = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({RabbitMQClientDepKey: client}))
    hook = RabbitMQShutdownHook()

    await hook(ctx)

    client.close.assert_awaited_once()


def test_rabbitmq_lifecycle_step_builds_hooks() -> None:
    config = RabbitMQConfig(prefetch_count=20)
    step = rabbitmq_lifecycle_step(dsn="amqp://guest:guest@localhost/", config=config)

    assert step.id == "rabbitmq_lifecycle"
    assert isinstance(step.startup, RabbitMQStartupHook)
    assert isinstance(step.shutdown, RabbitMQShutdownHook)


# ....................... #


class TestHeartbeatReachesTheBroker:
    """The configured heartbeat must ride on the DSN, not on a keyword.

    ``connect_robust(dsn, heartbeat=...)`` looks right and does nothing: aio-pika's
    ``make_url`` returns a supplied URL untouched and discards every other keyword with it,
    so the connection silently used the driver default for as long as a DSN was passed —
    which is always. Nothing caught it until aio-pika 10 dropped the keyword from its typed
    overloads. These assert the value is actually *on the URL* the driver receives.
    """

    def test_the_configured_heartbeat_is_a_query_parameter(self) -> None:
        from datetime import timedelta

        from forze_rabbitmq.kernel.client.client import (
            _with_heartbeat,  # pyright: ignore[reportPrivateUsage]
        )

        url = _with_heartbeat("amqp://guest:guest@localhost/", timedelta(seconds=45))

        assert url.query["heartbeat"] == "45"
        # The rest of the DSN survives intact — host, credentials and vhost.
        assert url.host == "localhost"
        assert url.user == "guest"
        assert url.path == "/"

    def test_it_preserves_other_query_parameters_and_the_vhost(self) -> None:
        from datetime import timedelta

        from forze_rabbitmq.kernel.client.client import (
            _with_heartbeat,  # pyright: ignore[reportPrivateUsage]
        )

        url = _with_heartbeat(
            "amqps://u:p@broker:5671/prod?name=writer&connection_timeout=3",
            timedelta(seconds=10),
        )

        assert url.query["heartbeat"] == "10"
        assert url.query["name"] == "writer"
        assert url.query["connection_timeout"] == "3"
        assert url.scheme == "amqps"
        assert url.port == 5671
        assert url.path == "/prod"

    def test_the_configured_value_overrides_one_already_in_the_dsn(self) -> None:
        """The config is the explicit setting, and it is the one that gets validated."""

        from datetime import timedelta

        from forze_rabbitmq.kernel.client.client import (
            _with_heartbeat,  # pyright: ignore[reportPrivateUsage]
        )

        url = _with_heartbeat(
            "amqp://guest:guest@localhost/?heartbeat=5", timedelta(seconds=60)
        )

        assert url.query["heartbeat"] == "60"

    def test_a_sub_second_heartbeat_does_not_truncate_to_zero(self) -> None:
        """AMQP reads heartbeat=0 as *disabled*, so truncation would silently turn it off.

        ``RabbitMQConfig`` already refuses a non-positive heartbeat, which is what keeps the
        integer conversion honest — this pins that the two agree.
        """

        from datetime import timedelta

        import pytest as _pytest

        from forze.base.exceptions import CoreException
        from forze_rabbitmq.kernel.client import RabbitMQConfig

        with _pytest.raises(CoreException, match="Heartbeat must be positive"):
            RabbitMQConfig(heartbeat=timedelta(0))

        # The smallest value the config accepts still survives conversion as a whole second.
        from forze_rabbitmq.kernel.client.client import (
            _with_heartbeat,  # pyright: ignore[reportPrivateUsage]
        )

        assert _with_heartbeat("amqp://h/", timedelta(seconds=1)).query["heartbeat"] == "1"
