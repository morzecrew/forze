"""``UpsertItem``: a mirrored item updates with its create payload, and ``skip_none`` leaves
the update's null fields out."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from pydantic import ConfigDict, Field

from forze.application.contracts.document import UpsertItem
from forze.domain.models import BaseDTO

pytestmark = pytest.mark.unit


class _Payload(BaseDTO):
    name: str | None = None
    note: str | None = None
    meta: dict[str, Any] | None = None
    tag: str = "dflt"


class TestUpsertItem:
    def test_a_mirror_updates_with_its_create_payload(self) -> None:
        payload = _Payload(name="a", note=None)
        item = UpsertItem.mirror(uuid4(), payload)

        assert item.create is payload and item.update is payload

    def test_skip_none_leaves_out_the_top_level_nulls_of_the_update_only(self) -> None:
        payload = _Payload(name="a", note=None, meta={"k": None})
        item = UpsertItem.mirror(uuid4(), payload, skip_none=True)

        # A mapping is not looked into, and the create is inserted as it is.
        assert item.update.model_fields_set == {"name", "meta"}
        assert item.update.model_dump(exclude_unset=True) == {"name": "a", "meta": {"k": None}}
        assert item.create is payload and payload.model_fields_set == {"name", "note", "meta"}

    def test_skip_none_applies_to_an_ordinary_item(self) -> None:
        item = UpsertItem(
            id=uuid4(), create=_Payload(), update=_Payload(note=None, tag="t"), skip_none=True
        )

        assert item.update.model_dump(exclude_unset=True) == {"tag": "t"}

    def test_an_update_without_nulls_is_kept_as_it_is(self) -> None:
        update = _Payload(name="a")

        assert UpsertItem(id=uuid4(), create=_Payload(), update=update, skip_none=True).update is (
            update
        )

    def test_skip_none_keeps_each_value_on_its_own_field_whatever_the_aliases(self) -> None:
        class _Aliased(BaseDTO):
            model_config = ConfigDict(populate_by_name=True)

            first: str | None = Field(default=None, alias="second")
            second: str | None = None

        # By field name: validating {"second": None} would fill ``first`` through its alias.
        update = _Aliased(first="kept").model_copy(update={"second": None})
        item = UpsertItem(id=uuid4(), create=_Aliased(), update=update, skip_none=True)

        assert item.update.first == "kept"
        assert item.update.model_fields_set == {"first"}
