"""Unit tests for :class:`forze_sqs.settings.SQSSettings` (no AWS I/O)."""

import attrs
import pytest
from pydantic import SecretStr, ValidationError

from forze.base.exceptions import CoreException

pytest.importorskip("aioboto3")

from forze_sqs.kernel.client import SQSConfig
from forze_sqs.settings import CLIENT_FIELDS, SQSSettings

# ----------------------- #


class TestCredentials:
    def test_neither_key_defers_to_the_credential_chain(self) -> None:
        settings = SQSSettings(endpoint="https://sqs.internal")

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
            SQSSettings(endpoint="https://sqs.internal", **kwargs)  # type: ignore[arg-type]

    # ....................... #

    def test_both_set_is_accepted(self) -> None:
        settings = SQSSettings(
            endpoint="https://sqs.internal",
            access_key_id="AKIA",
            secret_access_key=SecretStr("s"),
        )

        assert settings.secret_access_key is not None


# ....................... #


class TestEndpoint:
    def test_returns_the_stripped_endpoint(self) -> None:
        assert SQSSettings(endpoint=" https://sqs.internal ").require_endpoint() == (
            "https://sqs.internal"
        )

    # ....................... #

    @pytest.mark.parametrize("endpoint", [None, "", "   "])
    def test_requires_an_endpoint(self, endpoint: str | None) -> None:
        with pytest.raises(CoreException, match="SQS endpoint is required"):
            SQSSettings(endpoint=endpoint).require_endpoint()


# ....................... #


class TestConfig:
    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        assert SQSSettings(endpoint="https://sqs.internal").config == SQSConfig()

    # ....................... #

    def test_set_knobs_reach_the_client_config(self) -> None:
        config = SQSSettings(endpoint="https://sqs.internal", region_name="eu-west-1").config

        assert config.region_name == "eu-west-1"

    # ....................... #

    def test_field_names_match_the_client_config(self) -> None:
        """Fails the day a `SQSConfig` field is renamed out from under the settings."""

        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(SQSConfig)}
        assert set(CLIENT_FIELDS) <= set(SQSSettings.model_fields)
