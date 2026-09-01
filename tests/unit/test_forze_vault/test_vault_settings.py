"""Unit tests for :class:`forze_vault.settings.VaultSettings` (no Vault I/O)."""

import attrs
import pytest
from pydantic import SecretStr, ValidationError

from forze.base.exceptions import CoreException

pytest.importorskip("hvac")

from forze_vault.kernel.client import VaultConfig
from forze_vault.settings import CLIENT_FIELDS, VaultSettings

# ----------------------- #


class TestConfig:
    def test_carries_the_address_and_token_through(self) -> None:
        config = VaultSettings(url="https://vault.internal:8200", token=SecretStr("hvs.x")).config

        assert config.url == "https://vault.internal:8200"
        assert config.token.get_secret_value() == "hvs.x"

    # ....................... #

    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        settings = VaultSettings(url="https://vault.internal")

        assert settings.config == VaultConfig(url="https://vault.internal", token=SecretStr(""))

    # ....................... #

    def test_set_knobs_reach_the_client_config(self) -> None:
        config = VaultSettings(
            url="https://v",
            namespace="team-a",
            verify=False,
            retry_total=7,
        ).config

        assert (config.namespace, config.verify) == ("team-a", False)
        assert config.retry_total == 7

    # ....................... #

    @pytest.mark.parametrize("url", [None, "", "   "])
    def test_requires_a_url(self, url: str | None) -> None:
        with pytest.raises(CoreException, match="Vault url is required"):
            _ = VaultSettings(url=url).config

    # ....................... #

    @pytest.mark.parametrize("url", ["http://vault.internal:8200", "http://10.0.0.5:8200"])
    def test_refuses_plaintext_to_a_remote_host(self, url: str) -> None:
        """The token rides on every request and every response carries a secret, so
        plaintext to anything but this machine puts both on the wire — and `verify` cannot
        protect a connection that was never encrypted."""

        with pytest.raises(ValidationError, match="must be https"):
            VaultSettings(url=url, token=SecretStr("hvs.x"))

    # ....................... #

    @pytest.mark.parametrize("url", ["vault.internal", "ftp://v", "ftp://127.0.0.1:8200"])
    def test_refuses_a_url_that_is_not_http_at_all(self, url: str) -> None:
        """Checked before the loopback carve-out, which is why `ftp://127.0.0.1` is
        refused rather than waved through as "loopback, therefore fine"."""

        with pytest.raises(ValidationError, match="must start with https"):
            VaultSettings(url=url, token=SecretStr("hvs.x"))

    # ....................... #

    @pytest.mark.parametrize(
        "url",
        ["http://127.0.0.1:8200", "http://localhost:8200", "http://[::1]:8200"],
    )
    def test_allows_plaintext_to_loopback(self, url: str) -> None:
        """`vault server -dev` listens on `http://127.0.0.1:8200`, and a packet that never
        leaves the machine is not a cleartext transmission."""

        assert VaultSettings(url=url, token=SecretStr("hvs.x")).config.url == url

    # ....................... #

    def test_field_names_match_the_client_config(self) -> None:
        """Fails the day a `VaultConfig` field is renamed out from under the settings."""

        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(VaultConfig)}
        assert set(CLIENT_FIELDS) <= set(VaultSettings.model_fields)
