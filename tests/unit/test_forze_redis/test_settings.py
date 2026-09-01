"""Unit tests for :class:`forze_redis.settings.RedisSettings` (no Redis I/O)."""

from datetime import timedelta

import attrs
import pytest
from pydantic import SecretStr, ValidationError

from forze.base.exceptions import CoreException

pytest.importorskip("redis")

from forze_redis.kernel.client import RedisConfig
from forze_redis.settings import CLIENT_FIELDS, RedisSettings

# ----------------------- #


class TestDsn:
    def test_builds_the_full_endpoint(self) -> None:
        settings = RedisSettings(password=SecretStr("hunter2"), host="cache.internal", port=6379)

        assert settings.dsn.get_secret_value() == "redis://:hunter2@cache.internal:6379"

    # ....................... #

    def test_omits_credentials_when_there_is_no_password(self) -> None:
        """Not a bare `:@` — an ACL-enabled server refuses that as an empty login."""

        assert (
            RedisSettings(host="cache.internal").dsn.get_secret_value() == "redis://cache.internal"
        )

    # ....................... #

    def test_ssl_selects_the_secure_scheme(self) -> None:
        settings = RedisSettings(host="cache.internal", ssl=True)

        assert settings.dsn.get_secret_value().startswith("rediss://")

    # ....................... #

    def test_an_acl_username_reaches_the_url(self) -> None:
        """A managed Redis with ACLs enabled cannot be reached without one."""

        settings = RedisSettings(host="c", username="app", password=SecretStr("pw"))

        assert settings.dsn.get_secret_value() == "redis://app:pw@c"

    # ....................... #

    def test_a_username_without_a_password_is_refused(self) -> None:
        """`redis://app:@host` carries an empty password, so the client cannot
        authenticate as the user that was asked for."""

        with pytest.raises(ValidationError, match="username needs a password"):
            RedisSettings(host="c", username="app")

    # ....................... #

    def test_a_logical_database_becomes_the_path(self) -> None:
        assert RedisSettings(host="c", db=3).dsn.get_secret_value() == "redis://c/3"

    # ....................... #

    def test_database_zero_is_still_written(self) -> None:
        """`0` is a choice an operator made, not the absence of one."""

        assert RedisSettings(host="c", db=0).dsn.get_secret_value() == "redis://c/0"

    # ....................... #

    def test_percent_encodes_the_password(self) -> None:
        settings = RedisSettings(password=SecretStr("p@ss/word"), host="cache.internal")

        assert settings.dsn.get_secret_value() == "redis://:p%40ss%2Fword@cache.internal"

    # ....................... #

    def test_brackets_a_bare_ipv6_host(self) -> None:
        assert RedisSettings(host="::1", port=6379).dsn.get_secret_value() == "redis://[::1]:6379"

    # ....................... #

    @pytest.mark.parametrize("host", [None, "", "   "])
    def test_requires_a_host(self, host: str | None) -> None:
        """Whitespace counts as unset — otherwise the URL reaches DNS and fails there."""

        with pytest.raises(CoreException, match="Redis host is required"):
            _ = RedisSettings(host=host).dsn

    # ....................... #

    def test_rejects_an_out_of_range_port(self) -> None:
        with pytest.raises(ValidationError):
            RedisSettings(host="cache.internal", port=0)


# ....................... #


class TestConfig:
    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        """Including the timeouts: an unset variable must never disable one."""

        config = RedisSettings(host="cache.internal").config

        assert config == RedisConfig()
        assert config.socket_timeout == timedelta(seconds=5)

    # ....................... #

    def test_set_knobs_reach_the_pool_config(self) -> None:
        config = RedisSettings(
            host="cache.internal",
            max_size=5,
            socket_timeout=timedelta(seconds=2),
            connect_timeout=timedelta(seconds=1),
            client_name="orders-api",
        ).config

        assert config.max_size == 5
        assert config.socket_timeout == timedelta(seconds=2)
        assert config.connect_timeout == timedelta(seconds=1)
        assert config.client_name == "orders-api"

    # ....................... #

    def test_pool_field_names_match_the_pool_config(self) -> None:
        """Fails the day a `RedisConfig` field is renamed out from under the settings."""

        known = {field.name for field in attrs.fields(RedisConfig)}

        assert set(CLIENT_FIELDS) <= known
        assert set(CLIENT_FIELDS) <= set(RedisSettings.model_fields)

    # ....................... #

    def test_pool_validation_still_belongs_to_the_config(self) -> None:
        """A knob pydantic does not range-check, so the refusal is `RedisConfig`'s own."""

        with pytest.raises(CoreException, match="Socket timeout must be positive"):
            _ = RedisSettings(host="cache.internal", socket_timeout=timedelta(0)).config

    # ....................... #

    def test_an_out_of_range_pool_size_is_refused_by_the_config(self) -> None:
        """One guard, not two: the settings layer forwards, `RedisConfig` decides."""

        with pytest.raises(CoreException, match="Max size must be at least 1"):
            _ = RedisSettings(host="cache.internal", max_size=0).config
