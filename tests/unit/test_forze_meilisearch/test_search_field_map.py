"""The search gateway caches the logical<->physical field map (and its inverse).

``physical_path`` (per indexed field) and ``from_hit`` (per search hit) resolve the
map once at construction instead of rebuilding it on every element.
"""

import pytest
from pydantic import BaseModel

from forze.application.contracts.querying import QueryFilterLimits
from forze.application.contracts.search import SearchSpec
from forze.base.exceptions import CoreException
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


def test_the_spec_filter_limits_bound_the_rendered_filter() -> None:
    names = [f"n{i}" for i in range(1_500)]
    raised = MeilisearchSearchGateway(
        spec=SearchSpec(
            name="items",
            model_type=_Item,
            fields=["title"],
            filter_limits=QueryFilterLimits(max_in_size=2_000),
        ),
        config=MeilisearchSearchConfig(index_uid="items"),
    )

    assert raised.filter_renderer.render_filters({"$values": {"title": {"$in": names}}})

    with pytest.raises(CoreException, match="1000"):
        _gateway().filter_renderer.render_filters({"$values": {"title": {"$in": names}}})
