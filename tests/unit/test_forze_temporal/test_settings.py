"""Unit tests for :class:`forze_temporal.settings.TemporalSettings` (no server I/O)."""

import attrs
import pytest
from pydantic import SecretStr

from forze.base.exceptions import CoreException

pytest.importorskip("temporalio")

from forze_temporal.kernel.client import TemporalConfig
from forze_temporal.settings import CLIENT_FIELDS, TemporalSettings

# ----------------------- #


class TestAddress:
    def test_joins_host_and_port(self) -> None:
        assert TemporalSettings(host="t.internal", port=7233).address == "t.internal:7233"

    # ....................... #

    def test_requires_a_host(self) -> None:
        with pytest.raises(CoreException, match="Temporal host is required"):
            _ = TemporalSettings().address


# ....................... #


class TestConfig:
    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        assert TemporalSettings(host="t").config == TemporalConfig()

    # ....................... #

    def test_set_knobs_reach_the_client_config(self) -> None:
        config = TemporalSettings(host="t", namespace="orders", tls=True, lazy=True).config

        assert (config.namespace, config.tls, config.lazy) == ("orders", True, True)

    # ....................... #

    def test_an_api_key_without_tls_is_still_refused_by_the_config(self) -> None:
        """One guard, not two: the settings layer forwards, `TemporalConfig` decides."""

        with pytest.raises(CoreException):
            _ = TemporalSettings(host="t", api_key=SecretStr("k" * 32)).config

    # ....................... #

    def test_field_names_match_the_client_config(self) -> None:
        """Fails the day a `TemporalConfig` field is renamed out from under the settings."""

        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(TemporalConfig)}
        assert set(CLIENT_FIELDS) <= set(TemporalSettings.model_fields)
