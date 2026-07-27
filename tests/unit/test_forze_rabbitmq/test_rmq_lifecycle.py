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

    def test_a_positive_sub_second_heartbeat_is_refused(self) -> None:
        """The fail-open case: AMQP reads heartbeat=0 as *disabled*.

        A sub-second value passed the old "must be positive" check and then truncated to 0
        on the way to the URL, so the tightest-looking setting silently turned heartbeats
        off — the same shape as a maxTimeMS of 0 meaning unlimited. Refused at the config,
        which is what makes the int() conversion downstream lossless.
        """

        from datetime import timedelta

        import pytest as _pytest

        from forze.base.exceptions import CoreException
        from forze_rabbitmq.kernel.client import RabbitMQConfig

        for sub_second in (
            timedelta(milliseconds=1),
            timedelta(milliseconds=500),
            timedelta(milliseconds=999),
        ):
            with _pytest.raises(CoreException, match="at least 1s"):
                RabbitMQConfig(heartbeat=sub_second)

    def test_a_fractional_heartbeat_is_refused_rather_than_rounded(self) -> None:
        """1.5s is not 1s and not 2s; the wire drops the fraction, so silently picking one
        would give the operator a heartbeat they did not ask for."""

        from datetime import timedelta

        import pytest as _pytest

        from forze.base.exceptions import CoreException
        from forze_rabbitmq.kernel.client import RabbitMQConfig

        with _pytest.raises(CoreException, match="whole number of seconds"):
            RabbitMQConfig(heartbeat=timedelta(seconds=1, milliseconds=500))

    def test_a_non_positive_heartbeat_is_still_refused(self) -> None:
        from datetime import timedelta

        import pytest as _pytest

        from forze.base.exceptions import CoreException
        from forze_rabbitmq.kernel.client import RabbitMQConfig

        for bad in (timedelta(0), timedelta(seconds=-5)):
            with _pytest.raises(CoreException, match="at least 1s"):
                RabbitMQConfig(heartbeat=bad)

    def test_every_accepted_heartbeat_survives_conversion_intact(self) -> None:
        """The property the config validation exists to guarantee: whatever it accepts
        reaches the URL unchanged and never as 0."""

        from datetime import timedelta

        from forze_rabbitmq.kernel.client import RabbitMQConfig
        from forze_rabbitmq.kernel.client.client import (
            _with_heartbeat,  # pyright: ignore[reportPrivateUsage]
        )

        for seconds in (1, 2, 30, 60, 3600):
            config = RabbitMQConfig(heartbeat=timedelta(seconds=seconds))
            rendered = _with_heartbeat("amqp://h/", config.heartbeat).query["heartbeat"]

            assert rendered == str(seconds)
            assert rendered != "0"
