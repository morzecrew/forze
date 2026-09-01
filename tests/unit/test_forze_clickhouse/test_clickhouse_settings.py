"""Unit tests for :class:`forze_clickhouse.settings.ClickHouseSettings` (no server I/O)."""

import attrs
import pytest
from pydantic import SecretStr

from forze.base.exceptions import CoreException

pytest.importorskip("clickhouse_connect")

from forze_clickhouse.kernel.client import ClickHouseConfig
from forze_clickhouse.settings import CLIENT_FIELDS, SECURE_PORT, ClickHouseSettings

# ----------------------- #


class TestConfig:
    def test_carries_the_connection_through(self) -> None:
        config = ClickHouseSettings(
            host="ch.internal",
            port=8443,
            username="app",
            password=SecretStr("hunter2"),
            database="events",
            secure=True,
        ).config

        assert (config.host, config.port) == ("ch.internal", 8443)
        assert (config.username, config.database, config.secure) == ("app", "events", True)
        assert config.password.get_secret_value() == "hunter2"

    # ....................... #

    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        assert ClickHouseSettings(host="localhost").config == ClickHouseConfig()

    # ....................... #

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            ({}, 8123),
            ({"secure": True}, SECURE_PORT),
            ({"secure": True, "port": 9440}, 9440),
            ({"port": 9000}, 9000),
        ],
    )
    def test_tls_moves_the_default_port(self, kwargs: dict[str, object], expected: int) -> None:
        """`ClickHouseConfig` defaults to 8123 whether or not TLS is on, so a settings
        object that turned TLS on and named no port would reach the plaintext listener."""

        assert ClickHouseSettings(host="ch", **kwargs).config.port == expected  # type: ignore[arg-type]

    # ....................... #

    def test_refuses_an_unset_host(self) -> None:
        """Stricter than `ClickHouseConfig`, which defaults to localhost, and on purpose:
        a connection that silently falls back to whatever is listening locally is the
        failure this whole family of models exists to refuse."""

        with pytest.raises(CoreException, match="ClickHouse host is required"):
            _ = ClickHouseSettings().config

    # ....................... #

    def test_field_names_match_the_client_config(self) -> None:
        """Fails the day a `ClickHouseConfig` field is renamed out from under it."""

        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(ClickHouseConfig)}
        assert set(CLIENT_FIELDS) <= set(ClickHouseSettings.model_fields)
