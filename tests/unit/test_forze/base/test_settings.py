"""Unit tests for :class:`forze.base.settings.RuntimeSettings`."""

import pytest
from pydantic import ValidationError

from forze.base.logging import AccessLogMode, AccessLogSampler
from forze.base.settings import RuntimeSettings

# ----------------------- #


class TestRuntimeSettings:
    def test_defaults_are_the_deployed_shape(self) -> None:
        """Not ``bootstrap_logging``'s defaults — see the ``log_render`` docstring."""

        rt = RuntimeSettings()

        assert rt.log_level == "info"
        assert rt.log_render == "json"
        assert rt.access_log is AccessLogMode.SAMPLED
        assert rt.telemetry == "otlp"

    # ....................... #

    def test_full_version_joins_version_and_build(self) -> None:
        rt = RuntimeSettings(version="1.4.2", build_id="8891")

        assert rt.full_version == "1.4.2+8891"
        assert RuntimeSettings().full_version == "local+unknown"

    # ....................... #

    def test_full_version_is_serialized(self) -> None:
        """A computed field, so a dumped settings object carries it too."""

        assert RuntimeSettings(version="1.0", build_id="7").model_dump()["full_version"] == "1.0+7"

    # ....................... #

    @pytest.mark.parametrize(
        ("field", "raw", "expected"),
        [
            ("log_level", " WARNING ", "warning"),
            ("log_render", "JSON", "json"),
            ("telemetry", "Console", "console"),
            ("access_log", "FULL", AccessLogMode.FULL),
        ],
    )
    def test_environment_casing_is_folded(self, field: str, raw: str, expected: object) -> None:
        assert getattr(RuntimeSettings.model_validate({field: raw}), field) == expected

    # ....................... #

    def test_rejects_an_unknown_choice(self) -> None:
        with pytest.raises(ValidationError):
            RuntimeSettings.model_validate({"log_level": "verbose"})

    # ....................... #

    def test_access_log_feeds_the_sampler(self) -> None:
        """The point of the field: it is ``AccessLogSampler``'s argument, unconverted."""

        settings = RuntimeSettings(access_log=AccessLogMode.OFF)
        sampler = AccessLogSampler(mode=settings.access_log)

        assert sampler.should_log(subject="/orders", is_error=False) is False
