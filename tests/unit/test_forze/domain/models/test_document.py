"""Tests for forze.domain.models.document."""

from datetime import datetime
from uuid import UUID

import pytest

from forze.base.errors import ValidationError
from forze.base.primitives import JsonDict
from forze.domain.models import (
    CreateDocumentCmd,
    Document,
    DocumentHistory,
    ReadDocument,
)
from forze.domain.validation import update_validator

# ----------------------- #


class SampleDocument(Document):
    name: str
    value: int = 0


class TestDocumentBasics:
    def test_default_fields(self) -> None:
        doc = SampleDocument(name="test")
        assert isinstance(doc.id, UUID)
        assert doc.rev == 1
        assert isinstance(doc.created_at, datetime)
        assert isinstance(doc.last_update_at, datetime)

    def test_id_is_frozen(self) -> None:
        doc = SampleDocument(name="test")
        with pytest.raises(ValidationError):
            doc.update({"id": doc.id})

    def test_rev_is_frozen(self) -> None:
        doc = SampleDocument(name="test")
        with pytest.raises(ValidationError):
            doc.update({"rev": 2})

    def test_created_at_is_frozen(self) -> None:
        doc = SampleDocument(name="test")
        with pytest.raises(ValidationError):
            doc.update({"created_at": doc.created_at})


class TestDocumentUpdate:
    def test_applies_diff_and_bumps_last_update(self) -> None:
        doc = SampleDocument(name="old")
        before_update = doc.last_update_at
        after, diff = doc.update({"name": "new"})
        assert after.name == "new"
        assert "name" in diff
        assert "last_update_at" in diff
        assert after.last_update_at > before_update

    def test_no_op_update_returns_same_doc(self) -> None:
        doc = SampleDocument(name="same", value=42)
        after, diff = doc.update({"name": "same", "value": 42})
        assert diff == {}
        assert after is doc

    def test_unknown_field_raises(self) -> None:
        doc = SampleDocument(name="test")
        with pytest.raises(ValidationError):
            doc.update({"nonexistent": "val"})

    def test_multiple_fields_updated(self) -> None:
        doc = SampleDocument(name="old", value=1)
        after, diff = doc.update({"name": "new", "value": 99})
        assert after.name == "new"
        assert after.value == 99


class TestDocumentTouch:
    def test_updates_last_update_only(self) -> None:
        doc = SampleDocument(name="x")
        before = doc.last_update_at
        _, diff = doc.touch()
        assert "last_update_at" in diff
        assert doc.last_update_at >= before

    def test_touch_does_not_change_other_fields(self) -> None:
        doc = SampleDocument(name="original", value=42)
        doc.touch()
        assert doc.name == "original"
        assert doc.value == 42


class TestDocumentApplyUpdate:
    def test_empty_diff_returns_self(self) -> None:
        doc = SampleDocument(name="x")
        result = doc._apply_update({})
        assert result is doc

    def test_non_empty_diff_returns_new_instance(self) -> None:
        doc = SampleDocument(name="old")
        result = doc._apply_update({"name": "new"})
        assert result.name == "new"
        assert result is not doc


class TestDocumentUpdateValidators:
    def test_validators_run_on_update(self) -> None:
        calls: list[str] = []

        class ValidatedDoc(Document):
            name: str

            @update_validator
            def check_name(self, after: "ValidatedDoc", diff: JsonDict) -> None:
                calls.append(f"{self.name}->{after.name}")

        doc = ValidatedDoc(name="old")
        doc.update({"name": "new"})
        assert len(calls) == 1
        assert calls[0] == "old->new"

    def test_validators_run_on_empty_diff(self) -> None:
        calls: list[str] = []

        class ValidatedDoc(Document):
            name: str

            @update_validator
            def check(self, after: "ValidatedDoc", diff: JsonDict) -> None:
                calls.append("called")

        doc = ValidatedDoc(name="same")
        doc.update({"name": "same"})
        assert calls == ["called"]

    def test_field_scoped_validator_only_fires_for_relevant_fields(self) -> None:
        calls: list[str] = []

        class Doc(Document):
            name: str
            value: int = 0

            @update_validator(fields=["name"])
            def check_name(self, after: "Doc", diff: JsonDict) -> None:
                calls.append("name")

            @update_validator(fields=["value"])
            def check_value(self, after: "Doc", diff: JsonDict) -> None:
                calls.append("value")

        doc = Doc(name="a", value=1)
        doc.update({"value": 2})
        assert calls == ["value"]

    def test_validator_can_raise(self) -> None:
        class Doc(Document):
            name: str

            @update_validator
            def check(self, after: "Doc", diff: JsonDict) -> None:
                if after.name == "forbidden":
                    raise ValidationError("Name forbidden")

        doc = Doc(name="ok")
        with pytest.raises(ValidationError, match="forbidden"):
            doc.update({"name": "forbidden"})


class TestValidateHistoricalConsistency:
    def test_detects_conflict(self) -> None:
        old = SampleDocument(name="v1", value=1)
        current = SampleDocument(name="v2", value=1)
        data: JsonDict = {"name": "v3"}
        assert not current.validate_historical_consistency(old, data)

    def test_no_conflict_disjoint_fields(self) -> None:
        old = SampleDocument(name="v1", value=1)
        after, _ = old.update({"name": "v2"})
        data: JsonDict = {"value": 99}
        assert after.validate_historical_consistency(old, data)

    def test_same_change_no_conflict(self) -> None:
        old = SampleDocument(name="v1", value=1)
        after, _ = old.update({"name": "v2"})
        data: JsonDict = {"name": "v2"}
        assert after.validate_historical_consistency(old, data)


# ----------------------- #
# DTOs


class TestCreateDocumentCmd:
    def test_defaults(self) -> None:
        cmd = CreateDocumentCmd()
        assert cmd.id is None
        assert cmd.created_at is None

    def test_with_values(self) -> None:
        from forze.base.primitives.uuid import uuid7

        uid = uuid7()
        now = datetime.now()
        cmd = CreateDocumentCmd(id=uid, created_at=now)
        assert cmd.id == uid
        assert cmd.created_at == now


class TestReadDocument:
    def test_fields(self) -> None:
        from forze.base.primitives.uuid import uuid7

        uid = uuid7()
        now = datetime.now()
        rd = ReadDocument(id=uid, rev=3, created_at=now, last_update_at=now)
        assert rd.id == uid
        assert rd.rev == 3


class TestDocumentHistory:
    def test_fields(self) -> None:
        doc = SampleDocument(name="hist")
        h = DocumentHistory[SampleDocument](
            source="test",
            id=doc.id,
            rev=doc.rev,
            data=doc,
        )
        assert h.source == "test"
        assert h.data.name == "hist"
        assert isinstance(h.created_at, datetime)
