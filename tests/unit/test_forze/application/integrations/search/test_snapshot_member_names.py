"""A federated search's snapshot key, over members named by ``StrEnum`` values."""

from enum import StrEnum

from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze.application.integrations.search import SearchResultSnapshot
from forze.application.integrations.search.snapshot import federated_member_sorts

# ----------------------- #


class _Member(StrEnum):
    ORDERS = "orders"
    PEOPLE = "people"


class _Hit(BaseModel):
    id: str
    title: str


def _members(names: list[str]) -> list[SearchSpec[_Hit]]:
    return [
        SearchSpec(name=name, model_type=_Hit, fields=["title"], default_sort={"title": "asc"})
        for name in names
    ]


def test_members_named_by_str_enum_key_the_snapshot_as_their_values() -> None:
    """Every federated search fingerprints its members' default sorts; members named by a
    ``StrEnum`` failed it, so the search failed before it ran."""

    def fingerprint(names: list[str]) -> str:
        return SearchResultSnapshot.federated_fingerprint(
            "q", None, None, spec_name="fed", extras=federated_member_sorts(_members(names))
        )

    assert fingerprint([_Member.ORDERS, _Member.PEOPLE]) == fingerprint(["orders", "people"])
