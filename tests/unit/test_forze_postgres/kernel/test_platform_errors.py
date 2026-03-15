"""Unit tests for forze_postgres.kernel.platform.errors."""

from __future__ import annotations

import pytest

from forze.base.errors import NotFoundError
from forze_postgres.kernel.platform import errors as platform_errors


class _Diag:
    def __init__(self, message_detail: str) -> None:
        self.message_detail = message_detail


class _FakeForeignKeyViolation(Exception):
    def __init__(self, message_detail: str) -> None:
        super().__init__(message_detail)
        self.diag = _Diag(message_detail)


class TestPsycopgErrorHandler:
    """Tests for FK violation mapping in _psycopg_eh."""

    def test_fk_violation_maps_to_not_found_with_parsed_details(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Extract table/value details from psycopg FK violation messages."""
        monkeypatch.setattr(
            platform_errors.errors,
            "ForeignKeyViolation",
            _FakeForeignKeyViolation,
        )
        detail = (
            'Key (user_id)=(a57cf97f-a50f-42eb-bdc6-502f8c7f18af) '
            'is not present in table "users"'
        )

        err = platform_errors._psycopg_eh(_FakeForeignKeyViolation(detail), "create_doc")

        assert isinstance(err, NotFoundError)
        assert err.message == "Reference document not found."
        assert err.details == {
            "table": "users",
            "value": "a57cf97f-a50f-42eb-bdc6-502f8c7f18af",
        }

    def test_fk_violation_falls_back_to_raw_details(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Use raw psycopg detail when message cannot be parsed."""
        monkeypatch.setattr(
            platform_errors.errors,
            "ForeignKeyViolation",
            _FakeForeignKeyViolation,
        )
        detail = "insert or update on table orders violates foreign key constraint"

        err = platform_errors._psycopg_eh(_FakeForeignKeyViolation(detail), "create_doc")

        assert isinstance(err, NotFoundError)
        assert err.message == "Reference document not found."
        assert err.details == {"raw": detail}
