"""Unit tests for :class:`forze_vault.settings.VaultSettings` (no Vault I/O)."""

import attrs
import pytest
from pydantic import SecretStr

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
        config = VaultSettings(url="https://v", namespace="team-a", verify=False).config

        assert (config.namespace, config.verify) == ("team-a", False)

    # ....................... #

    @pytest.mark.parametrize("url", [None, "", "   "])
    def test_requires_a_url(self, url: str | None) -> None:
        with pytest.raises(CoreException, match="Vault url is required"):
            _ = VaultSettings(url=url).config

    # ....................... #

    def test_field_names_match_the_client_config(self) -> None:
        """Fails the day a `VaultConfig` field is renamed out from under the settings."""

        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(VaultConfig)}
        assert set(CLIENT_FIELDS) <= set(VaultSettings.model_fields)
