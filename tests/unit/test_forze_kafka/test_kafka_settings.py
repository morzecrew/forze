"""Unit tests for :class:`forze_kafka.settings.KafkaSettings` (no broker I/O)."""

import attrs
import pytest
from pydantic import SecretStr

from forze.base.exceptions import CoreException

pytest.importorskip("aiokafka")

from forze_kafka.kernel.client import KafkaConfig
from forze_kafka.settings import CLIENT_FIELDS, KafkaSettings

# ----------------------- #


class TestServers:
    def test_joins_the_seed_list(self) -> None:
        settings = KafkaSettings(bootstrap_servers=("a:9092", "b:9092"))

        assert settings.servers == "a:9092,b:9092"

    # ....................... #

    def test_drops_blank_entries(self) -> None:
        assert KafkaSettings(bootstrap_servers=("a:9092", "  ", "")).servers == "a:9092"

    # ....................... #

    @pytest.mark.parametrize("servers", [(), ("",), ("   ", "\t")])
    def test_requires_at_least_one_broker(self, servers: tuple[str, ...]) -> None:
        """An empty join is `""`, which aiokafka accepts and resolves to localhost:9092 —
        a broker nobody configured, reached without an error."""

        with pytest.raises(CoreException, match="Kafka bootstrap_servers is required"):
            _ = KafkaSettings(bootstrap_servers=servers).servers


# ....................... #


class TestConfig:
    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        assert KafkaSettings(bootstrap_servers=("a",)).config == KafkaConfig()

    # ....................... #

    def test_set_knobs_reach_the_client_config(self) -> None:
        config = KafkaSettings(
            bootstrap_servers=("a",),
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username="app",
            sasl_plain_password=SecretStr("hunter2"),
        ).config

        assert (config.security_protocol, config.sasl_mechanism) == ("SASL_SSL", "SCRAM-SHA-256")
        assert config.sasl_plain_password is not None
        assert config.sasl_plain_password.get_secret_value() == "hunter2"

    # ....................... #

    def test_field_names_match_the_client_config(self) -> None:
        """Fails the day a `KafkaConfig` field is renamed out from under the settings."""

        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(KafkaConfig)}
        assert set(CLIENT_FIELDS) <= set(KafkaSettings.model_fields)

    # ....................... #

    def test_delivery_guarantees_are_not_settings(self) -> None:
        """An environment variable must not be able to turn idempotent production off."""

        assert not {"acks", "enable_idempotence"} & set(KafkaSettings.model_fields)
