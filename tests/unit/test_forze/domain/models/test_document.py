"""Tests for forze.domain.models.document."""

from forze.base.exceptions import CoreException, exc
from datetime import datetime, timezone
from uuid import UUID

import pytest

from forze.base.primitives import JsonDict
from forze.domain.models import (
    AggregateRoot,
    CoreModel,
    CreateDocumentCmd,
    Document,
    DocumentHistory,
    DomainEvent,
    ReadDocument,
    event_emitter,
    invariant,
)
from forze.domain.validation import update_validator

# ----------------------- #

class SampleDocument(Document):
    name: str
    value: int = 0

class Address(CoreModel):
    street: str
    city: str

class NestedDocument(Document):
    address: Address
    due: datetime

class TestDocumentBasics:
    def test_default_fields(self) -> None:
        doc = SampleDocument(name="test")
        assert isinstance(doc.id, UUID)
        assert doc.rev == 1
        assert isinstance(doc.created_at, datetime)
        assert isinstance(doc.last_update_at, datetime)

    def test_id_is_frozen(self) -> None:
        doc = SampleDocument(name="test")
        with pytest.raises(CoreException):
            doc.update({"id": doc.id})

    def test_rev_is_frozen(self) -> None:
        doc = SampleDocument(name="test")
        with pytest.raises(CoreException):
            doc.update({"rev": 2})

    def test_created_at_is_frozen(self) -> None:
        doc = SampleDocument(name="test")
        with pytest.raises(CoreException):
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
        with pytest.raises(CoreException):
            doc.update({"nonexistent": "val"})

    def test_multiple_fields_updated(self) -> None:
        doc = SampleDocument(name="old", value=1)
        after, diff = doc.update({"name": "new", "value": 99})
        assert after.name == "new"
        assert after.value == 99

class TestDocumentUpdateCanonicalization:
    """Update merges into python-mode state and fully re-validates the result."""

    def test_same_datetime_object_is_a_noop(self) -> None:
        # Regression: a python datetime equal to the stored value must not produce
        # a spurious diff (previously compared against a json-mode ISO string).
        due = datetime(2026, 1, 1, tzinfo=timezone.utc)
        doc = NestedDocument(address=Address(street="main", city="LA"), due=due)
        before_update = doc.last_update_at

        after, diff = doc.update({"due": due})

        assert diff == {}
        assert after is doc
        assert after.rev == doc.rev
        assert after.last_update_at == before_update

    def test_noop_update_does_not_fire_field_matched_emitters(self) -> None:
        class _DueMoved(DomainEvent):
            pass

        class Task(Document, AggregateRoot):
            due: datetime

            @event_emitter(fields=["due"])
            def _due_moved(self, after: "Task", diff: JsonDict) -> DomainEvent | None:
                return _DueMoved()

        due = datetime(2026, 1, 1, tzinfo=timezone.utc)
        task = Task(due=due)

        after, diff = task.update({"due": due})

        assert diff == {}
        assert after.has_pending_events is False

        # Sanity: a real change still fires the emitter.
        moved, diff = task.update({"due": datetime(2027, 1, 1, tzinfo=timezone.utc)})
        assert "due" in diff
        assert [type(e) for e in moved.collect_events()] == [_DueMoved]

    def test_partial_nested_dict_merges_and_validates(self) -> None:
        doc = NestedDocument(
            address=Address(street="main", city="LA"),
            due=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        after, diff = doc.update({"address": {"city": "NY"}})

        # The instance carries a fully validated nested model, siblings preserved.
        assert isinstance(after.address, Address)
        assert after.address.city == "NY"
        assert after.address.street == "main"

        # The diff keeps its merge-patch shape: only the changed nested key.
        assert diff["address"] == {"city": "NY"}
        assert "last_update_at" in diff
        assert set(diff) == {"address", "last_update_at"}

    def test_iso_string_patch_yields_real_datetime(self) -> None:
        doc = NestedDocument(
            address=Address(street="main", city="LA"),
            due=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        after, diff = doc.update({"due": "2027-05-04T00:00:00Z"})

        assert isinstance(after.due, datetime)
        assert after.due == datetime(2027, 5, 4, tzinfo=timezone.utc)
        assert isinstance(diff["due"], datetime)

    def test_iso_string_equal_to_current_value_is_a_noop(self) -> None:
        doc = NestedDocument(
            address=Address(street="main", city="LA"),
            due=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        after, diff = doc.update({"due": "2026-01-01T00:00:00Z"})

        assert diff == {}
        assert after is doc

    def test_validators_and_invariants_observe_validated_instance(self) -> None:
        seen: list[str] = []

        class Doc(Document):
            address: Address

            @invariant
            def _city_not_empty(self) -> None:
                # Crashes with AttributeError if address is a plain (partial) dict.
                assert self.address.city

            @update_validator(fields=["address"])
            def _check_address(self, after: "Doc", diff: JsonDict) -> None:
                assert isinstance(after.address, Address)
                seen.append(after.address.city)

        doc = Doc(address=Address(street="main", city="LA"))
        after, _ = doc.update({"address": {"city": "NY"}})

        assert seen == ["NY"]
        assert after.address.street == "main"

    def test_computed_fields_never_appear_in_diff(self) -> None:
        # Regression: re-validation recomputes @computed_field values on the
        # candidate; they must not leak into the diff (gateways would try to
        # write them as table columns — not-persisted by contract).
        from pydantic import computed_field

        class Doc(Document):
            value: int

            @computed_field  # type: ignore[prop-decorator]
            @property
            def doubled(self) -> int:
                return self.value * 2

        doc = Doc(value=2)
        after, diff = doc.update({"value": 5})

        assert "doubled" not in diff
        assert set(diff) == {"value", "last_update_at"}
        assert after.doubled == 10

    def test_materialized_computed_field_appears_in_diff_when_changed(self) -> None:
        # A materialized computed field is persisted, so when its inputs change the
        # recomputed value must appear in the diff (diff-applying gateways write it).
        from pydantic import computed_field

        class Doc(Document):
            value: int

            @computed_field  # type: ignore[prop-decorator]
            @property
            def doubled(self) -> int:
                return self.value * 2

        doc = Doc(value=2)
        after, diff = doc.update({"value": 5}, materialized=frozenset({"doubled"}))

        assert diff["doubled"] == 10
        assert set(diff) == {"value", "doubled", "last_update_at"}
        assert after.doubled == 10

    def test_materialized_field_absent_from_diff_when_unchanged(self) -> None:
        # The derived value only enters the diff when it actually changes.
        from pydantic import computed_field

        class Doc(Document):
            value: int
            note: str = ""

            @computed_field  # type: ignore[prop-decorator]
            @property
            def doubled(self) -> int:
                return self.value * 2

        doc = Doc(value=2, note="a")
        _, diff = doc.update({"note": "b"}, materialized=frozenset({"doubled"}))

        assert "doubled" not in diff
        assert set(diff) == {"note", "last_update_at"}

    def test_nested_computed_fields_excluded_from_dump_and_diff(self) -> None:
        # Regression: the round-1 fix only stripped *top-level* computed fields
        # (the explicit model_fields filter). A @computed_field declared on a
        # nested model leaked through `_dump_stored_fields` and therefore into
        # update diffs whenever the nested field was patched.
        from pydantic import computed_field

        class GeoAddress(CoreModel):
            street: str
            city: str

            @computed_field  # type: ignore[prop-decorator]
            @property
            def label(self) -> str:
                return f"{self.street}, {self.city}"

        class Doc(Document):
            address: GeoAddress

        doc = Doc(address=GeoAddress(street="main", city="LA"))

        dump = doc._dump_stored_fields()
        assert "label" not in dump["address"]

        after, diff = doc.update({"address": {"city": "NY"}})
        assert diff["address"] == {"city": "NY"}
        assert "label" not in diff["address"]
        assert after.address.label == "main, NY"

class TestMaterializeUpdateIsolation:
    """Pin the aliasing safety of the scoped per-value deepcopy in
    ``_materialize_update``: updated container values must not alias the
    caller's patch containers or the before-document's containers (pydantic
    passes ``Any``-typed dict/list values through validation unchanged)."""

    def test_updated_dict_field_isolated_from_caller_patch(self) -> None:
        class Doc(Document):
            meta: JsonDict = {}

        doc = Doc(meta={"a": {"b": 1}})
        patch: JsonDict = {"meta": {"a": {"b": 2}, "new": {"x": 1}}}

        after, diff = doc.update(patch)
        assert after.meta == {"a": {"b": 2}, "new": {"x": 1}}

        # Mutating the caller's patch containers must not leak into the result.
        patch["meta"]["new"]["x"] = 999
        patch["meta"]["a"]["b"] = 999
        assert after.meta["new"]["x"] == 1
        assert after.meta["a"]["b"] == 2

        # Mutating the BEFORE document's nested containers must not leak either.
        doc.meta["a"]["b"] = 777
        assert after.meta["a"]["b"] == 2

        # And vice versa: mutating the result touches neither patch nor before.
        after.meta["a"]["b"] = -1
        assert doc.meta["a"]["b"] == 777
        assert patch["meta"]["a"]["b"] == 999

        # The diff itself is independent of all three as well.
        assert diff["meta"] == {"a": {"b": 2}, "new": {"x": 1}}

    def test_updated_list_field_isolated_from_caller_patch(self) -> None:
        from typing import Any

        class Doc(Document):
            entries: list[Any] = []

        doc = Doc(entries=[{"a": 1}])
        patch: JsonDict = {"entries": [{"a": 2}, {"b": 3}]}

        after, _ = doc.update(patch)

        patch["entries"][0]["a"] = 999
        patch["entries"].append({"c": 4})
        assert after.entries == [{"a": 2}, {"b": 3}]

        after.entries[0]["a"] = -1
        after.entries.append("x")
        assert doc.entries == [{"a": 1}]
        assert patch["entries"][0]["a"] == 999

    def test_updated_nested_model_isolated_from_before_and_patch(self) -> None:
        doc = NestedDocument(
            address=Address(street="main", city="LA"),
            due=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        patch: JsonDict = {"address": {"city": "NY"}}

        after, _ = doc.update(patch)

        patch["address"]["city"] = "MUT"
        assert after.address.city == "NY"

        # Mutating the result's nested model leaves the before document intact.
        after.address.city = "SF"
        assert doc.address.city == "LA"


class TestUpdateSingleModelCopy:
    """Pin that ``update`` performs exactly one ``model_copy`` (the
    ``last_update_at`` bump is folded into the materialize copy) and never
    requests a whole-model deep copy."""

    def test_scalar_and_nested_updates_copy_once_and_shallow(self) -> None:
        copies: list[tuple[tuple[str, ...], bool]] = []

        class SpyDoc(Document):
            name: str
            address: Address

            def model_copy(  # type: ignore[override]
                self,
                *,
                update: dict[str, object] | None = None,
                deep: bool = False,
            ) -> "SpyDoc":
                copies.append((tuple(update or ()), deep))
                return super().model_copy(update=update, deep=deep)

        doc = SpyDoc(name="a", address=Address(street="main", city="LA"))

        copies.clear()
        after, _ = doc.update({"name": "b"})
        assert after.name == "b"
        assert len(copies) == 1
        keys, deep = copies[0]
        assert set(keys) == {"name", "last_update_at"}
        assert deep is False

        copies.clear()
        after, _ = doc.update({"address": {"city": "NY"}})
        assert after.address.city == "NY"
        assert len(copies) == 1
        keys, deep = copies[0]
        assert set(keys) == {"address", "last_update_at"}
        assert deep is False

    def test_noop_update_copies_zero_times(self) -> None:
        copies: list[bool] = []

        class SpyDoc(Document):
            name: str

            def model_copy(  # type: ignore[override]
                self,
                *,
                update: dict[str, object] | None = None,
                deep: bool = False,
            ) -> "SpyDoc":
                copies.append(deep)
                return super().model_copy(update=update, deep=deep)

        doc = SpyDoc(name="same")
        copies.clear()
        after, diff = doc.update({"name": "same"})
        assert diff == {}
        assert after is doc
        assert copies == []


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
                    raise exc.validation("Name forbidden")

        doc = Doc(name="ok")
        with pytest.raises(CoreException, match="forbidden"):
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

    def test_noop_datetime_resend_of_concurrently_changed_field_no_conflict(
        self,
    ) -> None:
        # THE false-positive regression: old/self used to be dumped json-mode
        # while `data` is python-mode, so a re-sent identical datetime looked
        # like a touch (datetime != ISO string) and a no-op echo of a field
        # another writer concurrently changed raised a false conflict.
        due = datetime(2026, 1, 1, tzinfo=timezone.utc)
        old = NestedDocument(address=Address(street="main", city="LA"), due=due)
        current, _ = old.update({"due": datetime(2027, 2, 2, tzinfo=timezone.utc)})

        # Stale client echoes exactly what it read: no intent to change `due`.
        assert current.validate_historical_consistency(old, {"due": due})

    def test_noop_uuid_resend_of_concurrently_changed_field_no_conflict(self) -> None:
        from forze.base.primitives.uuid import uuid7

        class RefDoc(Document):
            ref: UUID

        ref = uuid7()
        old = RefDoc(ref=ref)
        current, _ = old.update({"ref": uuid7()})

        assert current.validate_historical_consistency(old, {"ref": ref})

    def test_genuine_datetime_conflict_still_flagged(self) -> None:
        # A *different* value for a concurrently-changed field is a real conflict.
        due = datetime(2026, 1, 1, tzinfo=timezone.utc)
        old = NestedDocument(address=Address(street="main", city="LA"), due=due)
        current, _ = old.update({"due": datetime(2027, 2, 2, tzinfo=timezone.utc)})

        data: JsonDict = {"due": datetime(2028, 3, 3, tzinfo=timezone.utc)}
        assert not current.validate_historical_consistency(old, data)

    def test_same_datetime_change_no_conflict(self) -> None:
        # Concurrent agreement: both writers set the same datetime. The
        # compatible-scalar-overlap rule used to break for datetimes because
        # the values were compared across modes (ISO string vs datetime).
        due = datetime(2026, 1, 1, tzinfo=timezone.utc)
        new_due = datetime(2027, 2, 2, tzinfo=timezone.utc)
        old = NestedDocument(address=Address(street="main", city="LA"), due=due)
        current, _ = old.update({"due": new_due})

        assert current.validate_historical_consistency(old, {"due": new_due})

    def test_non_overlapping_python_mode_changes_pass(self) -> None:
        due = datetime(2026, 1, 1, tzinfo=timezone.utc)
        old = NestedDocument(address=Address(street="main", city="LA"), due=due)
        current, _ = old.update({"due": datetime(2027, 2, 2, tzinfo=timezone.utc)})

        assert current.validate_historical_consistency(
            old, {"address": {"city": "NY"}}
        )

    def test_nested_scalar_conflict_still_flagged(self) -> None:
        # Hybrid merge-patch semantics are unchanged: overlapping nested
        # scalar paths with different values still conflict.
        due = datetime(2026, 1, 1, tzinfo=timezone.utc)
        old = NestedDocument(address=Address(street="main", city="LA"), due=due)
        current, _ = old.update({"address": {"city": "NY"}})

        assert not current.validate_historical_consistency(
            old, {"address": {"city": "SF"}}
        )

    def test_container_conflict_still_flagged(self) -> None:
        # Hybrid semantics: container touches on overlapping paths conflict
        # even when scalar equality cannot be established.
        class TaggedDoc(Document):
            tags: list[str]

        old = TaggedDoc(tags=["a"])
        current, _ = old.update({"tags": ["a", "b"]})

        assert not current.validate_historical_consistency(old, {"tags": ["c"]})

# ----------------------- #
# DTOs

class TestCreateDocumentCmd:
    def test_is_empty_deprecated_alias(self) -> None:
        # Identity/timestamps no longer live on the create payload; the symbol is a
        # deprecated empty BaseDTO subclass kept for back-compat.
        cmd = CreateDocumentCmd()
        assert not type(cmd).model_fields

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
