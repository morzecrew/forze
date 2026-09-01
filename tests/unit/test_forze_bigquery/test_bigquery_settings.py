"""Unit tests for :class:`forze_bigquery.settings.BigQuerySettings` (no BigQuery I/O)."""

import attrs
import pytest

from forze.base.exceptions import CoreException

pytest.importorskip("gcloud.aio.bigquery")

from forze_bigquery.kernel.client import BigQueryConfig
from forze_bigquery.settings import CLIENT_FIELDS, BigQuerySettings

# ----------------------- #


class TestSettings:
    @pytest.mark.parametrize("project_id", [None, "", "   "])
    def test_requires_a_project_id(self, project_id: str | None) -> None:
        with pytest.raises(CoreException, match="BigQuery project_id is required"):
            BigQuerySettings(project_id=project_id).require_project_id()

    # ....................... #

    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        assert BigQuerySettings(project_id="acme").config == BigQueryConfig()

    # ....................... #

    def test_the_billing_ceiling_reaches_the_client_config(self) -> None:
        """The setting an operator most wants in the environment: a runaway query's cost."""

        settings = BigQuerySettings(project_id="acme", maximum_bytes_billed=10**11)

        assert settings.config.maximum_bytes_billed == 10**11

    # ....................... #

    def test_field_names_match_the_client_config(self) -> None:
        """Fails the day a `BigQueryConfig` field is renamed out from under the settings."""

        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(BigQueryConfig)}
        assert set(CLIENT_FIELDS) <= set(BigQuerySettings.model_fields)
