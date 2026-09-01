"""Unit tests for :class:`forze_duckdb.settings.DuckDbSettings` (no DuckDB I/O)."""

import attrs
import pytest
from pydantic import ValidationError

pytest.importorskip("duckdb")

from forze_duckdb.kernel.client import DuckDbConfig
from forze_duckdb.settings import CLIENT_FIELDS, DuckDbSettings

# ----------------------- #


class TestSettings:
    def test_defaults_to_an_in_memory_database(self) -> None:
        assert DuckDbSettings().database == ":memory:"

    # ....................... #

    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        assert DuckDbSettings().config == DuckDbConfig()

    # ....................... #

    def test_the_memory_limit_reaches_the_client_config(self) -> None:
        """The setting that stops a query getting the process OOM-killed in a container."""

        assert DuckDbSettings(memory_limit="4GB").config.memory_limit == "4GB"

    # ....................... #

    @pytest.mark.parametrize("kwargs", [{"threads": 0}, {"max_concurrent_queries": 0}])
    def test_non_positive_limits_are_refused(self, kwargs: dict[str, int]) -> None:
        with pytest.raises(ValidationError):
            DuckDbSettings(**kwargs)

    # ....................... #

    def test_field_names_match_the_client_config(self) -> None:
        """Fails the day a `DuckDbConfig` field is renamed out from under the settings."""

        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(DuckDbConfig)}
        assert set(CLIENT_FIELDS) <= set(DuckDbSettings.model_fields)
