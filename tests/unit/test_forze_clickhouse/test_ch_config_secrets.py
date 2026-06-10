"""Secret-handling tests for ClickHouse config and routing credentials."""

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import SecretStr

from forze_clickhouse.kernel.client.client import ClickHouseClient
from forze_clickhouse.kernel.client.routing_credentials import (
    ClickHouseRoutingCredentials,
    routing_fingerprint,
)
from forze_clickhouse.kernel.client.value_objects import ClickHouseConfig

# ----------------------- #

_PASSWORD = "hunter2-super-secret"


class TestClickHouseConfigPassword:
    def test_str_input_is_coerced_to_secret(self) -> None:
        config = ClickHouseConfig(password=_PASSWORD)  # type: ignore[arg-type]

        assert isinstance(config.password, SecretStr)
        assert config.password.get_secret_value() == _PASSWORD

    def test_secret_input_is_accepted(self) -> None:
        config = ClickHouseConfig(password=SecretStr(_PASSWORD))

        assert config.password.get_secret_value() == _PASSWORD

    def test_repr_does_not_leak_password(self) -> None:
        config = ClickHouseConfig(password=_PASSWORD)  # type: ignore[arg-type]

        assert _PASSWORD not in repr(config)
        assert _PASSWORD not in str(config)


class TestRoutingCredentialsPassword:
    def test_str_input_is_coerced_to_secret(self) -> None:
        creds = ClickHouseRoutingCredentials(password=_PASSWORD)  # type: ignore[arg-type]

        assert isinstance(creds.password, SecretStr)
        assert creds.password.get_secret_value() == _PASSWORD

    def test_repr_does_not_leak_password(self) -> None:
        creds = ClickHouseRoutingCredentials(password=_PASSWORD)  # type: ignore[arg-type]

        assert _PASSWORD not in repr(creds)
        assert _PASSWORD not in str(creds)
        assert _PASSWORD not in creds.model_dump_json()

    def test_to_clickhouse_config_keeps_secret(self) -> None:
        creds = ClickHouseRoutingCredentials(password=_PASSWORD)  # type: ignore[arg-type]
        config = creds.to_clickhouse_config()

        assert config.password.get_secret_value() == _PASSWORD


class TestRoutingFingerprint:
    def test_fingerprint_never_embeds_raw_password(self) -> None:
        creds = ClickHouseRoutingCredentials(password=_PASSWORD)  # type: ignore[arg-type]

        assert _PASSWORD not in routing_fingerprint(creds)

    def test_fingerprint_stable_for_same_password(self) -> None:
        a = ClickHouseRoutingCredentials(password=_PASSWORD)  # type: ignore[arg-type]
        b = ClickHouseRoutingCredentials(password=SecretStr(_PASSWORD))

        assert routing_fingerprint(a) == routing_fingerprint(b)

    def test_fingerprint_changes_on_rotation(self) -> None:
        a = ClickHouseRoutingCredentials(password=_PASSWORD)  # type: ignore[arg-type]
        b = ClickHouseRoutingCredentials(password="rotated")  # type: ignore[arg-type]

        assert routing_fingerprint(a) != routing_fingerprint(b)

    def test_empty_password_matches_omitted(self) -> None:
        a = ClickHouseRoutingCredentials()
        b = ClickHouseRoutingCredentials(password="")  # type: ignore[arg-type]

        assert routing_fingerprint(a) == routing_fingerprint(b)


@pytest.mark.asyncio
async def test_client_connect_receives_raw_password() -> None:
    client = ClickHouseClient()
    config = ClickHouseConfig(password=_PASSWORD)  # type: ignore[arg-type]
    captured: dict[str, Any] = {}

    async def _fake_create(**kwargs: Any) -> Any:
        captured.update(kwargs)
        return AsyncMock()

    with patch(
        "forze_clickhouse.kernel.client.client.create_async_client",
        side_effect=_fake_create,
    ):
        await client.initialize(config)

    assert captured["password"] == _PASSWORD
