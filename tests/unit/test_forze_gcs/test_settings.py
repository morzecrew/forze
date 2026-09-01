"""Unit tests for :class:`forze_gcs.settings.GCSSettings` (no GCS I/O)."""

import attrs
import pytest

from forze.base.exceptions import CoreException

pytest.importorskip("gcloud.aio.storage")

from forze_gcs.kernel.client import GCSConfig
from forze_gcs.settings import CLIENT_FIELDS, GCSSettings

# ----------------------- #


class TestSettings:
    def test_returns_the_stripped_project_id(self) -> None:
        assert GCSSettings(project_id=" acme-prod ").require_project_id() == "acme-prod"

    # ....................... #

    @pytest.mark.parametrize("project_id", [None, "", "   "])
    def test_requires_a_project_id(self, project_id: str | None) -> None:
        with pytest.raises(CoreException, match="GCS project_id is required"):
            GCSSettings(project_id=project_id).require_project_id()

    # ....................... #

    def test_unset_knobs_keep_the_client_defaults(self) -> None:
        assert GCSSettings(project_id="acme").config == GCSConfig()

    # ....................... #

    def test_set_knobs_reach_the_client_config(self) -> None:
        settings = GCSSettings(project_id="acme", service_file="/run/secrets/sa.json")

        assert settings.config.service_file == "/run/secrets/sa.json"

    # ....................... #

    def test_field_names_match_the_client_config(self) -> None:
        """Fails the day a `GCSConfig` field is renamed out from under the settings."""

        assert set(CLIENT_FIELDS) <= {field.name for field in attrs.fields(GCSConfig)}
        assert set(CLIENT_FIELDS) <= set(GCSSettings.model_fields)
