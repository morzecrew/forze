"""Unit tests for :class:`forze_http.settings.HttpSettings` (no network I/O)."""

import attrs
import pytest
from pydantic import SecretStr

pytest.importorskip("httpx")

from forze_http.kernel.client import HttpConfig
from forze_http.settings import CLIENT_FIELDS, HttpSettings

# ----------------------- #


class TestSettings:
    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        assert HttpSettings().config == HttpConfig()

    # ....................... #

    def test_set_knobs_reach_the_client_config(self) -> None:
        assert HttpSettings(max_response_bytes=1024).config.max_response_bytes == 1024

    # ....................... #

    def test_a_base_url_is_optional(self) -> None:
        """A client handed absolute URLs needs none, so there is no `require_*` for it."""

        assert HttpSettings().base_url is None
        assert not hasattr(HttpSettings(), "require_base_url")

    # ....................... #

    def test_credentials_stay_out_of_the_repr(self) -> None:
        """A header value can itself be a credential, so both are masked."""

        settings = HttpSettings(
            auth_token=SecretStr("bearer-value"),
            default_headers={"X-Api-Key": "header-value"},
        )

        assert "bearer-value" not in repr(settings)
        assert "header-value" not in repr(settings)

        # `repr=False` alone would still have let `model_dump()` emit the header.
        assert "default_headers" not in settings.model_dump()

    # ....................... #

    def test_field_names_match_the_client_config(self) -> None:
        """Fails the day an `HttpConfig` field is renamed out from under the settings."""

        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(HttpConfig)}
        assert set(CLIENT_FIELDS) <= set(HttpSettings.model_fields)
