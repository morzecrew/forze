"""Unit tests for :class:`forze.base.settings.RuntimeSettings`."""

import pytest
from pydantic import BaseModel, ValidationError

from forze.base.exceptions import CoreException
from forze.base.logging import AccessLogMode, AccessLogSampler
from forze.base.settings import (
    EndpointSettings,
    RuntimeSettings,
    configured_fields,
    require,
)

# ----------------------- #


class _Knobs(BaseModel):
    """Stand-in for an integration settings model with two optional passthrough knobs."""

    size: int | None = None
    name: str | None = None
    unrelated: str = "kept out of the overrides"


# ....................... #


class TestRequire:
    def test_returns_the_stripped_value(self) -> None:
        assert require("  db.internal ", service="Postgres", setting="host") == "db.internal"

    # ....................... #

    @pytest.mark.parametrize("value", [None, "", "   ", "\t\n"])
    def test_refuses_an_absent_value_by_name(self, value: str | None) -> None:
        """Whitespace counts as unset — otherwise it reaches the backend and fails there."""

        with pytest.raises(CoreException, match="Meilisearch api_key is required"):
            require(value, service="Meilisearch", setting="api_key")


# ....................... #


class TestConfiguredFields:
    def test_drops_the_unset_ones(self) -> None:
        """The whole point: an unset knob is absent, not forwarded as `None`."""

        assert configured_fields(_Knobs(size=4), ("size", "name")) == {"size": 4}

    # ....................... #

    def test_forwards_every_set_one(self) -> None:
        knobs = _Knobs(size=0, name="")

        assert configured_fields(knobs, ("size", "name")) == {"size": 0, "name": ""}

    # ....................... #

    def test_ignores_fields_it_was_not_asked_for(self) -> None:
        """`unrelated` is set, and absent from the result because it was not named."""

        assert configured_fields(_Knobs(size=1), ("size",)) == {"size": 1}


# ....................... #


class TestEndpointSettings:
    def test_authority_joins_host_and_port(self) -> None:
        assert EndpointSettings(host="db", port=5432).authority(service="X") == "db:5432"

    # ....................... #

    def test_authority_omits_an_unset_port(self) -> None:
        assert EndpointSettings(host="db").authority(service="X") == "db"

    # ....................... #

    def test_authority_brackets_a_bare_ipv6_host(self) -> None:
        """Unbracketed, the literal's first colon reads as the port separator."""

        assert EndpointSettings(host="::1", port=9042).authority(service="X") == "[::1]:9042"

    # ....................... #

    def test_authority_leaves_an_already_bracketed_host_alone(self) -> None:
        assert EndpointSettings(host="[fe80::1]").authority(service="X") == "[fe80::1]"

    # ....................... #

    @pytest.mark.parametrize("host", [None, "", "   "])
    def test_refuses_a_blank_host_naming_the_service(self, host: str | None) -> None:
        settings = EndpointSettings(host=host)

        with pytest.raises(CoreException, match="Cassandra host is required"):
            settings.authority(service="Cassandra")

        with pytest.raises(CoreException, match="Cassandra host is required"):
            settings.require_host(service="Cassandra")

    # ....................... #

    @pytest.mark.parametrize("port", [0, 65536, -1])
    def test_rejects_an_out_of_range_port(self, port: int) -> None:
        with pytest.raises(ValidationError):
            EndpointSettings(host="db", port=port)


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
