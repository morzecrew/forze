"""Unit tests for :class:`forze_neo4j.settings.Neo4jSettings` (no Neo4j I/O).

The shared authority grammar is proven once against `EndpointSettings` in
`test_forze/base/test_settings.py`. What is here is the four-way scheme choice and the
auth pair.
"""

import attrs
import pytest
from pydantic import SecretStr

from forze.base.exceptions import CoreException

pytest.importorskip("neo4j")

from forze_neo4j.kernel.client import Neo4jConfig
from forze_neo4j.settings import CLIENT_FIELDS, Neo4jSettings

# ----------------------- #


class TestUri:
    @pytest.mark.parametrize(
        ("routing", "ssl", "expected"),
        [
            (True, False, "neo4j://g.internal:7687"),
            (True, True, "neo4j+s://g.internal:7687"),
            (False, False, "bolt://g.internal:7687"),
            (False, True, "bolt+s://g.internal:7687"),
        ],
    )
    def test_the_scheme_is_the_two_by_two_choice(
        self, routing: bool, ssl: bool, expected: str
    ) -> None:
        settings = Neo4jSettings(host="g.internal", port=7687, routing=routing, ssl=ssl)

        assert settings.uri.get_secret_value() == expected

    # ....................... #

    def test_routing_is_the_default(self) -> None:
        """A direct driver against a cluster loses routing without failing."""

        assert Neo4jSettings(host="g").uri.get_secret_value().startswith("neo4j://")

    # ....................... #

    def test_requires_a_host(self) -> None:
        with pytest.raises(CoreException, match="Neo4j host is required"):
            _ = Neo4jSettings().uri


# ....................... #


class TestAuth:
    def test_none_when_neither_is_set(self) -> None:
        assert Neo4jSettings(host="g").auth is None

    # ....................... #

    def test_the_pair_when_both_are_set(self) -> None:
        settings = Neo4jSettings(host="g", user="neo4j", password=SecretStr("hunter2"))

        assert settings.auth == ("neo4j", "hunter2")

    # ....................... #

    @pytest.mark.parametrize(
        "kwargs",
        [{"user": "neo4j"}, {"password": SecretStr("hunter2")}],
    )
    def test_half_set_is_refused(self, kwargs: dict[str, object]) -> None:
        """A user with no password authenticates as nobody, reported as a bad credential."""

        with pytest.raises(CoreException, match="both user and password"):
            _ = Neo4jSettings(host="g", **kwargs).auth  # type: ignore[arg-type]


# ....................... #


class TestConfig:
    def test_unset_knobs_keep_the_driver_defaults(self) -> None:
        assert Neo4jSettings(host="g").config == Neo4jConfig()

    # ....................... #

    def test_set_knobs_reach_the_driver_config(self) -> None:
        assert Neo4jSettings(host="g", database="orders").config.database == "orders"

    # ....................... #

    def test_field_names_match_the_driver_config(self) -> None:
        """Fails the day a `Neo4jConfig` field is renamed out from under the settings."""

        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(Neo4jConfig)}
        assert set(CLIENT_FIELDS) <= set(Neo4jSettings.model_fields)
