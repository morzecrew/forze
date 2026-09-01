"""Unit tests for :class:`forze_rabbitmq.settings.RabbitMQSettings` (no broker I/O).

The shared authority grammar is proven once against `EndpointSettings` in
`test_forze/base/test_settings.py`. What is here is the vhost escaping, which is the
reason this module exists.
"""

import attrs
import pytest
from pydantic import SecretStr

from forze.base.exceptions import CoreException

pytest.importorskip("aio_pika")

from forze_rabbitmq.kernel.client import RabbitMQConfig
from forze_rabbitmq.settings import CLIENT_FIELDS, RabbitMQSettings

# ----------------------- #


class TestDsn:
    def test_the_default_vhost_is_escaped(self) -> None:
        """`/` is the vhost's *name*; a literal `/` here addresses the empty-named one."""

        settings = RabbitMQSettings(host="mq.internal", port=5672)

        assert settings.dsn.get_secret_value() == "amqp://guest:guest@mq.internal:5672/%2F"

    # ....................... #

    def test_a_named_vhost_is_escaped_too(self) -> None:
        settings = RabbitMQSettings(host="mq.internal", vhost="orders/prod")

        assert settings.dsn.get_secret_value().endswith("/orders%2Fprod")

    # ....................... #

    def test_ssl_selects_the_secure_scheme(self) -> None:
        settings = RabbitMQSettings(host="mq.internal", ssl=True)

        assert settings.dsn.get_secret_value().startswith("amqps://")

    # ....................... #

    def test_percent_encodes_credentials(self) -> None:
        settings = RabbitMQSettings(host="mq", user="a/b", password=SecretStr("p@ss"))

        assert settings.dsn.get_secret_value() == "amqp://a%2Fb:p%40ss@mq/%2F"

    # ....................... #

    def test_requires_a_host(self) -> None:
        with pytest.raises(CoreException, match="RabbitMQ host is required"):
            _ = RabbitMQSettings().dsn


# ....................... #


class TestConfig:
    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        assert RabbitMQSettings(host="mq").config == RabbitMQConfig()

    # ....................... #

    def test_set_knobs_reach_the_client_config(self) -> None:
        assert RabbitMQSettings(host="mq", prefetch_count=10).config.prefetch_count == 10

    # ....................... #

    def test_field_names_match_the_client_config(self) -> None:
        """Fails the day a `RabbitMQConfig` field is renamed out from under the settings."""

        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(RabbitMQConfig)}
        assert set(CLIENT_FIELDS) <= set(RabbitMQSettings.model_fields)

    # ....................... #

    def test_delivery_semantics_are_not_settings(self) -> None:
        """An operator must not be able to turn durability off from the environment."""

        assert not {"publisher_confirms", "persistent_messages", "queue_durable"} & set(
            RabbitMQSettings.model_fields
        )
