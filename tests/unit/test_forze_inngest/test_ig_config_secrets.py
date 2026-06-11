"""Secret-handling tests for Inngest config keys."""

from unittest.mock import MagicMock, patch

from pydantic import SecretStr

from forze_inngest.kernel.client.client import InngestClient
from forze_inngest.kernel.client.config import InngestConfig

# ----------------------- #

_EVENT_KEY = "evt-key-super-secret"
_SIGNING_KEY = "signkey-test-super-secret"


class TestInngestConfigSecrets:
    def test_str_input_is_coerced_to_secret(self) -> None:
        config = InngestConfig(event_key=_EVENT_KEY, signing_key=_SIGNING_KEY)  # type: ignore[arg-type]

        assert isinstance(config.event_key, SecretStr)
        assert isinstance(config.signing_key, SecretStr)
        assert config.event_key.get_secret_value() == _EVENT_KEY
        assert config.signing_key.get_secret_value() == _SIGNING_KEY

    def test_secret_input_is_accepted(self) -> None:
        config = InngestConfig(
            event_key=SecretStr(_EVENT_KEY),
            signing_key=SecretStr(_SIGNING_KEY),
        )

        assert config.event_key is not None
        assert config.signing_key is not None
        assert config.event_key.get_secret_value() == _EVENT_KEY
        assert config.signing_key.get_secret_value() == _SIGNING_KEY

    def test_none_stays_none(self) -> None:
        config = InngestConfig()

        assert config.event_key is None
        assert config.signing_key is None

    def test_repr_does_not_leak_keys(self) -> None:
        config = InngestConfig(event_key=_EVENT_KEY, signing_key=_SIGNING_KEY)  # type: ignore[arg-type]

        assert _EVENT_KEY not in repr(config)
        assert _SIGNING_KEY not in repr(config)
        assert _EVENT_KEY not in str(config)
        assert _SIGNING_KEY not in str(config)


class TestInngestClientSdkBoundary:
    def test_sdk_receives_unwrapped_keys(self) -> None:
        config = InngestConfig(event_key=_EVENT_KEY, signing_key=_SIGNING_KEY)  # type: ignore[arg-type]

        with patch(
            "forze_inngest.kernel.client.client.inngest.Inngest",
        ) as sdk_factory:
            sdk_factory.return_value = MagicMock()
            InngestClient(app_id="app", config=config)

        kwargs = sdk_factory.call_args.kwargs

        assert kwargs["event_key"] == _EVENT_KEY
        assert kwargs["signing_key"] == _SIGNING_KEY

    def test_sdk_receives_none_when_keys_absent(self) -> None:
        with patch(
            "forze_inngest.kernel.client.client.inngest.Inngest",
        ) as sdk_factory:
            sdk_factory.return_value = MagicMock()
            InngestClient(app_id="app", config=InngestConfig())

        kwargs = sdk_factory.call_args.kwargs

        assert kwargs["event_key"] is None
        assert kwargs["signing_key"] is None
