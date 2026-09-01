"""Unit tests for :class:`forze_postgres.settings.PostgresSettings` (no DB I/O)."""

from datetime import timedelta

import attrs
import pytest
from pydantic import SecretStr, ValidationError

from forze.base.exceptions import CoreException

pytest.importorskip("psycopg")

from forze_postgres.kernel.client import PostgresConfig
from forze_postgres.settings import CLIENT_FIELDS, PostgresSettings

# ----------------------- #


class TestDsn:
    def test_builds_the_full_endpoint(self) -> None:
        settings = PostgresSettings(
            user="app",
            password=SecretStr("hunter2"),
            database="orders",
            host="db.internal",
            port=5432,
        )

        assert settings.dsn.get_secret_value() == "postgresql://app:hunter2@db.internal:5432/orders"

    # ....................... #

    def test_omits_the_port_when_unset(self) -> None:
        settings = PostgresSettings(host="db.internal")

        assert settings.dsn.get_secret_value() == "postgresql://postgres:@db.internal/postgres"

    # ....................... #

    def test_ssl_is_actually_read(self) -> None:
        """The regression this module exists for: `ssl` was declared and never used.

        `verify-full`, not `require`: `require` encrypts and authenticates nothing, and
        every sibling model's `ssl=True` verifies the certificate.
        """

        plain = PostgresSettings(host="db.internal").dsn.get_secret_value()
        secure = PostgresSettings(host="db.internal", ssl=True).dsn.get_secret_value()

        assert "sslmode" not in plain
        assert secure.endswith("?sslmode=verify-full")

    # ....................... #

    def test_percent_encodes_credentials(self) -> None:
        """A generated password routinely contains `@` or `/`, which re-parse the URI."""

        settings = PostgresSettings(
            user="a/b",
            password=SecretStr("p@ss:w/rd"),
            host="db.internal",
        )

        assert (
            settings.dsn.get_secret_value()
            == "postgresql://a%2Fb:p%40ss%3Aw%2Frd@db.internal/postgres"
        )

    # ....................... #

    def test_brackets_a_bare_ipv6_host(self) -> None:
        settings = PostgresSettings(host="::1", port=5432)

        assert settings.dsn.get_secret_value().endswith("@[::1]:5432/postgres")

    # ....................... #

    def test_percent_encodes_the_database_name(self) -> None:
        assert (
            PostgresSettings(host="db", database="my db")
            .dsn.get_secret_value()
            .endswith("/my%20db")
        )

    # ....................... #

    @pytest.mark.parametrize("host", [None, "", "   "])
    def test_requires_a_host(self, host: str | None) -> None:
        """Whitespace counts as unset — otherwise the DSN reaches DNS and fails there."""

        with pytest.raises(CoreException, match="Postgres host is required"):
            _ = PostgresSettings(host=host).dsn

    # ....................... #

    def test_rejects_an_out_of_range_port(self) -> None:
        with pytest.raises(ValidationError):
            PostgresSettings(host="db.internal", port=70000)


# ....................... #


class TestConfig:
    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        """No second copy of a default: an unset field is dropped, not defaulted here."""

        assert PostgresSettings(host="db.internal").config == PostgresConfig()

    # ....................... #

    def test_set_knobs_reach_the_pool_config(self) -> None:
        config = PostgresSettings(
            host="db.internal",
            min_size=1,
            max_size=4,
            statement_timeout=timedelta(seconds=3),
            lock_timeout=timedelta(seconds=1),
            application_name="orders-api",
        ).config

        assert (config.min_size, config.max_size) == (1, 4)
        assert config.statement_timeout == timedelta(seconds=3)
        assert config.lock_timeout == timedelta(seconds=1)
        assert config.application_name == "orders-api"

    # ....................... #

    def test_pool_field_names_match_the_pool_config(self) -> None:
        """Fails the day a `PostgresConfig` field is renamed out from under the settings."""

        known = {field.name for field in attrs.fields(PostgresConfig)}

        assert set(CLIENT_FIELDS) <= known
        assert set(CLIENT_FIELDS) <= set(PostgresSettings.model_fields)

    # ....................... #

    def test_pool_validation_still_belongs_to_the_config(self) -> None:
        with pytest.raises(CoreException, match="Minimum size must be less"):
            _ = PostgresSettings(host="db.internal", min_size=5, max_size=3).config
