"""Unit tests for :class:`forze_inngest.settings.InngestSettings` (no Inngest I/O)."""

import attrs
import pytest
from pydantic import SecretStr

pytest.importorskip("inngest")

from forze_inngest.kernel.client import InngestConfig
from forze_inngest.settings import CLIENT_FIELDS, InngestSettings

# ----------------------- #


class TestSettings:
    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        assert InngestSettings().config == InngestConfig()

    # ....................... #

    def test_the_two_keys_stay_separate(self) -> None:
        """They face opposite directions — one sends events, one authenticates Inngest."""

        config = InngestSettings(
            event_key=SecretStr("evt"),
            signing_key=SecretStr("sig"),
        ).config

        assert config.event_key is not None and config.signing_key is not None
        assert config.event_key.get_secret_value() == "evt"
        assert config.signing_key.get_secret_value() == "sig"

    # ....................... #

    def test_keys_stay_out_of_the_repr(self) -> None:
        assert "evt" not in repr(InngestSettings(event_key=SecretStr("evt")))

    # ....................... #

    def test_field_names_match_the_client_config(self) -> None:
        """Fails the day an `InngestConfig` field is renamed out from under the settings."""

        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(InngestConfig)}
        assert set(CLIENT_FIELDS) <= set(InngestSettings.model_fields)
