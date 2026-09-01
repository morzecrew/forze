"""Unit tests for :class:`forze_s3.settings.S3Settings` (no AWS I/O)."""

import attrs
import pytest
from pydantic import SecretStr, ValidationError

from forze.base.exceptions import CoreException

pytest.importorskip("aioboto3")

from forze_s3.kernel.client import S3Config
from forze_s3.settings import CLIENT_FIELDS, S3Settings

# ----------------------- #


class TestCredentials:
    def test_neither_key_defers_to_the_credential_chain(self) -> None:
        settings = S3Settings(endpoint="https://s3.internal")

        assert settings.access_key_id is None
        assert settings.secret_access_key is None

    # ....................... #

    @pytest.mark.parametrize(
        "kwargs",
        [{"access_key_id": "AKIA"}, {"secret_access_key": SecretStr("s")}],
    )
    def test_half_set_credentials_are_refused(self, kwargs: dict[str, object]) -> None:
        """Refused where it can still name the environment variable that is missing,
        rather than later from inside the AWS client."""

        with pytest.raises(ValidationError, match="both access_key_id and secret_access_key"):
            S3Settings(endpoint="https://s3.internal", **kwargs)  # type: ignore[arg-type]

    # ....................... #

    def test_both_set_is_accepted(self) -> None:
        settings = S3Settings(
            endpoint="https://s3.internal",
            access_key_id="AKIA",
            secret_access_key=SecretStr("s"),
        )

        assert settings.secret_access_key is not None


# ....................... #


class TestEndpoint:
    def test_returns_the_stripped_endpoint(self) -> None:
        assert S3Settings(endpoint=" https://s3.internal ").require_endpoint() == (
            "https://s3.internal"
        )

    # ....................... #

    @pytest.mark.parametrize("endpoint", [None, "", "   "])
    def test_requires_an_endpoint(self, endpoint: str | None) -> None:
        with pytest.raises(CoreException, match="S3 endpoint is required"):
            S3Settings(endpoint=endpoint).require_endpoint()


# ....................... #


class TestConfig:
    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        assert S3Settings(endpoint="https://s3.internal").config == S3Config()

    # ....................... #

    def test_set_knobs_reach_the_client_config(self) -> None:
        config = S3Settings(endpoint="https://s3.internal", region_name="eu-west-1").config

        assert config.region_name == "eu-west-1"

    # ....................... #

    def test_field_names_match_the_client_config(self) -> None:
        """Fails the day an `S3Config` field is renamed out from under the settings."""

        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(S3Config)}
        assert set(CLIENT_FIELDS) <= set(S3Settings.model_fields)
