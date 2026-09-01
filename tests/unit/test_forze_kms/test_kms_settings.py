"""Unit tests for the three cloud-KMS settings models (no KMS I/O)."""

import attrs
import pytest
from pydantic import SecretStr, ValidationError

# ----------------------- #


class TestAwsKmsSettings:
    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        pytest.importorskip("aioboto3")

        from forze_kms.aws.kernel.client import AwsKmsConfig
        from forze_kms.aws.settings import CLIENT_FIELDS, AwsKmsSettings

        assert AwsKmsSettings().config == AwsKmsConfig()
        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(AwsKmsConfig)}
        assert set(CLIENT_FIELDS) <= set(AwsKmsSettings.model_fields)

    # ....................... #

    @pytest.mark.parametrize(
        "kwargs",
        [{"access_key_id": "AKIA"}, {"secret_access_key": SecretStr("s")}],
    )
    def test_half_set_credentials_are_refused(self, kwargs: dict[str, object]) -> None:
        pytest.importorskip("aioboto3")

        from forze_kms.aws.settings import AwsKmsSettings

        with pytest.raises(ValidationError, match="both access_key_id and secret_access_key"):
            AwsKmsSettings(**kwargs)  # type: ignore[arg-type]


# ....................... #


class TestGcpKmsSettings:
    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        pytest.importorskip("google.cloud.kms")

        from forze_kms.gcp.kernel.client import GcpKmsConfig
        from forze_kms.gcp.settings import CLIENT_FIELDS, GcpKmsSettings

        assert GcpKmsSettings().config == GcpKmsConfig()
        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(GcpKmsConfig)}
        assert set(CLIENT_FIELDS) <= set(GcpKmsSettings.model_fields)


# ....................... #


class TestYcKmsSettings:
    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        pytest.importorskip("yandexcloud")

        from forze_kms.yc.kernel.client import YcKmsConfig
        from forze_kms.yc.settings import CLIENT_FIELDS, YcKmsSettings

        assert YcKmsSettings().config == YcKmsConfig()
        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(YcKmsConfig)}
        assert set(CLIENT_FIELDS) <= set(YcKmsSettings.model_fields)

    # ....................... #

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"iam_token": SecretStr("i"), "oauth_token": SecretStr("o")},
            {"iam_token": SecretStr("i"), "service_account_key": {"k": "v"}},
            {"oauth_token": SecretStr("o"), "service_account_key": {"k": "v"}},
        ],
    )
    def test_two_credentials_are_refused(self, kwargs: dict[str, object]) -> None:
        """Not twice the authentication — a question about which one wins."""

        pytest.importorskip("yandexcloud")

        from forze_kms.yc.settings import YcKmsSettings

        with pytest.raises(ValidationError, match="not several"):
            YcKmsSettings(**kwargs)  # type: ignore[arg-type]

    # ....................... #

    def test_the_service_account_key_never_reaches_a_dump(self) -> None:
        """It holds a private key, and no `SecretStr` wraps a mapping."""

        pytest.importorskip("yandexcloud")

        from forze_kms.yc.settings import YcKmsSettings

        settings = YcKmsSettings(service_account_key={"private_key": "-----BEGIN"})

        assert "service_account_key" not in settings.model_dump()
        assert "BEGIN" not in repr(settings)
        assert settings.service_account_key == {"private_key": "-----BEGIN"}

    # ....................... #

    def test_the_secret_token_feeds_the_step_directly(self) -> None:
        """No `.get_secret_value()` at the call site — the step unwraps it."""

        pytest.importorskip("yandexcloud")

        from forze_kms.yc import yckms_lifecycle_step
        from forze_kms.yc.settings import YcKmsSettings

        settings = YcKmsSettings(iam_token=SecretStr("t0ken"))
        step = yckms_lifecycle_step(iam_token=settings.iam_token, config=settings.config)

        assert step.startup is not None
        assert getattr(step.startup, "iam_token") == "t0ken"
