"""Unit tests for :class:`forze_firestore.settings.FirestoreSettings` (no Firestore I/O)."""

import pytest

from forze.base.exceptions import CoreException

pytest.importorskip("google.cloud.firestore")

from forze_firestore.settings import FirestoreSettings

# ----------------------- #


class TestSettings:
    def test_returns_the_stripped_project_id(self) -> None:
        assert FirestoreSettings(project_id=" acme-prod ").require_project_id() == "acme-prod"

    # ....................... #

    @pytest.mark.parametrize("project_id", [None, "", "   "])
    def test_requires_a_project_id(self, project_id: str | None) -> None:
        with pytest.raises(CoreException, match="Firestore project_id is required"):
            FirestoreSettings(project_id=project_id).require_project_id()

    # ....................... #

    def test_the_unnamed_database_is_the_default(self) -> None:
        """Firestore's own name for it, parentheses included."""

        assert FirestoreSettings(project_id="acme").database == "(default)"
