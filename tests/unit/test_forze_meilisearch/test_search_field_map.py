"""The search gateway caches the logical<->physical field map (and its inverse).

``physical_path`` (per indexed field) and ``from_hit`` (per search hit) resolve the
map once at construction instead of rebuilding it on every element.
"""

from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze_meilisearch.adapters.search.base import MeilisearchSearchGateway
from forze_meilisearch.execution.deps.configs import MeilisearchSearchConfig


class _Item(BaseModel):
    id: str
    title: str = ""
    body: str = ""


def _gateway() -> MeilisearchSearchGateway[_Item]:
    return MeilisearchSearchGateway(
        spec=SearchSpec(name="items", model_type=_Item, fields=["title", "body"]),
        config=MeilisearchSearchConfig(
            index_uid="items",
            field_map={"title": "title_phys", "body": "body_phys"},
        ),
    )


def test_cached_maps_are_built_once_and_correct() -> None:
    gw = _gateway()

    # Forward (logical -> physical) and inverse (physical -> logical) caches.
    assert gw._field_map_cache == {"title": "title_phys", "body": "body_phys"}  # type: ignore[reportPrivateUsage]
    assert gw._inv_field_map_cache == {"title_phys": "title", "body_phys": "body"}  # type: ignore[reportPrivateUsage]


def test_physical_path_uses_forward_map() -> None:
    gw = _gateway()

    assert gw.physical_path("title") == "title_phys"
    assert gw.physical_path("body") == "body_phys"
    # Unmapped fields pass through unchanged.
    assert gw.physical_path("id") == "id"


def test_from_hit_inverts_map_and_drops_meta_keys() -> None:
    gw = _gateway()

    hit = {
        "title_phys": "hello",
        "body_phys": "world",
        "id": "abc",
        "_rankingScore": 0.9,
        "_formatted": {},
    }

    assert gw.from_hit(hit) == {"title": "hello", "body": "world", "id": "abc"}


def test_from_hit_is_stable_across_calls() -> None:
    gw = _gateway()
    hit = {"title_phys": "x", "id": "1"}

    first = gw.from_hit(hit)
    second = gw.from_hit(hit)

    assert first == second == {"title": "x", "id": "1"}
