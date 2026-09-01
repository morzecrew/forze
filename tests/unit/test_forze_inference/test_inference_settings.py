"""Unit tests for the two inference settings models (no network I/O)."""

import pytest
from pydantic import SecretStr, ValidationError

from forze.base.exceptions import CoreException

# ----------------------- #


class TestInferenceHttpSettings:
    def test_requires_a_base_url(self) -> None:
        pytest.importorskip("httpx")

        from forze_inference.http.settings import InferenceHttpSettings

        with pytest.raises(CoreException, match="Inference HTTP base_url is required"):
            InferenceHttpSettings().require_base_url()

    # ....................... #

    def test_the_token_becomes_a_bearer_header(self) -> None:
        pytest.importorskip("httpx")

        from forze_inference.http.settings import InferenceHttpSettings

        settings = InferenceHttpSettings(base_url="https://m", auth_token=SecretStr("tkn"))

        assert settings.headers == {"Authorization": "Bearer tkn"}

    # ....................... #

    def test_an_explicit_authorization_header_wins(self) -> None:
        """Silently replacing a header a deployment spelled out is the harder failure."""

        pytest.importorskip("httpx")

        from forze_inference.http.settings import InferenceHttpSettings

        settings = InferenceHttpSettings(
            base_url="https://m",
            auth_token=SecretStr("tkn"),
            default_headers={"Authorization": "Basic abc"},
        )

        assert settings.headers["Authorization"] == "Basic abc"

    # ....................... #

    @pytest.mark.parametrize("name", ["Authorization", "authorization", "AUTHORIZATION"])
    def test_an_explicit_header_wins_whatever_its_casing(self, name: str) -> None:
        """HTTP header names are case-insensitive; a second one is a conflicting one."""

        pytest.importorskip("httpx")

        from forze_inference.http.settings import InferenceHttpSettings

        settings = InferenceHttpSettings(
            base_url="https://m",
            auth_token=SecretStr("tkn"),
            default_headers={name: "Basic abc"},
        )

        assert settings.headers == {name: "Basic abc"}

    # ....................... #

    def test_credentials_stay_out_of_the_repr(self) -> None:
        pytest.importorskip("httpx")

        from forze_inference.http.settings import InferenceHttpSettings

        settings = InferenceHttpSettings(
            base_url="https://m",
            auth_token=SecretStr("tkn"),
            default_headers={"X-Api-Key": "header-value"},
        )

        assert "tkn" not in repr(settings)
        assert "header-value" not in repr(settings)

    # ....................... #

    def test_a_non_positive_timeout_is_refused(self) -> None:
        pytest.importorskip("httpx")

        from forze_inference.http.settings import InferenceHttpSettings

        with pytest.raises(ValidationError):
            InferenceHttpSettings(base_url="https://m", timeout_s=0)


# ....................... #


class TestSageMakerSettings:
    @pytest.mark.parametrize(
        "kwargs",
        [{"access_key_id": "AKIA"}, {"secret_access_key": SecretStr("s")}],
    )
    def test_half_set_credentials_are_refused(self, kwargs: dict[str, object]) -> None:
        pytest.importorskip("aioboto3")

        from forze_inference.sagemaker.settings import SageMakerSettings

        with pytest.raises(ValidationError, match="both access_key_id and secret_access_key"):
            SageMakerSettings(**kwargs)  # type: ignore[arg-type]

    # ....................... #

    def test_neither_defers_to_the_credential_chain(self) -> None:
        pytest.importorskip("aioboto3")

        from forze_inference.sagemaker.settings import SageMakerSettings

        assert SageMakerSettings(region_name="eu-west-1").access_key_id is None
