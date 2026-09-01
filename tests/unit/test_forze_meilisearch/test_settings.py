"""Unit tests for :class:`forze_meilisearch.settings.MeilisearchSettings` (no server I/O)."""

import attrs
import pytest
from pydantic import SecretStr

from forze.base.exceptions import CoreException

pytest.importorskip("meilisearch_python_sdk")

from forze_meilisearch.kernel.client import MeilisearchConfig
from forze_meilisearch.settings import CLIENT_FIELDS, MeilisearchSettings

# ----------------------- #


class TestUrl:
    def test_builds_the_base_url(self) -> None:
        assert MeilisearchSettings(host="s.internal", port=7700).url == "http://s.internal:7700"

    # ....................... #

    def test_ssl_selects_https(self) -> None:
        assert MeilisearchSettings(host="s.internal", ssl=True).url == "https://s.internal"

    # ....................... #

    def test_is_not_a_secret(self) -> None:
        """No credential is in it — the key travels in a header — so nothing to mask."""

        assert isinstance(MeilisearchSettings(host="s").url, str)
        assert isinstance(MeilisearchSettings(host="s", api_key=SecretStr("k")).api_key, SecretStr)

    # ....................... #

    def test_requires_a_host(self) -> None:
        with pytest.raises(CoreException, match="Meilisearch host is required"):
            _ = MeilisearchSettings().url


# ....................... #


class TestConfig:
    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        assert MeilisearchSettings(host="s").config == MeilisearchConfig()

    # ....................... #

    def test_field_names_match_the_client_config(self) -> None:
        """Fails the day a `MeilisearchConfig` field is renamed out from under it."""

        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(MeilisearchConfig)}
        assert set(CLIENT_FIELDS) <= set(MeilisearchSettings.model_fields)
