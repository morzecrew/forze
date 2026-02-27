from forze.base.primitives import JsonDict
from forze.domain.models import Document


class SampleDocument(Document):
    name: str


def test_document_update_applies_diff_and_bumps_last_update() -> None:
    doc = SampleDocument(name="old")
    before_last_update = doc.last_update_at

    after, diff = doc.update({"name": "new"})

    assert after.name == "new"
    assert "name" in diff
    assert "last_update_at" in diff
    assert after.last_update_at > before_last_update


def test_document_update_rejects_frozen_field_changes() -> None:
    doc = SampleDocument(name="x")

    # id is frozen, attempting to change it should raise ValidationError
    from forze.base.errors import ValidationError

    try:
        doc.update({"id": doc.id})
    except ValidationError:
        pass
    else:  # pragma: no cover - defensive
        assert False, "Updating frozen field should raise ValidationError"


def test_document_touch_updates_last_update_only() -> None:
    doc = SampleDocument(name="x")
    before = doc.last_update_at

    _, diff = doc.touch()
    assert "last_update_at" in diff
    assert doc.last_update_at > before


def test_validate_historical_consistency_detects_conflict() -> None:
    old = SampleDocument(name="v1")
    current = SampleDocument(name="v2")

    # data that would update old to v3 conflicts with current (=v2)
    data: JsonDict = {"name": "v3"}
    consistent = current.validate_historical_consistency(old, data)

    assert not consistent
