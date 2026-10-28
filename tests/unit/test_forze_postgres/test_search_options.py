"""Unit tests for Postgres search option normalization."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel, TypeAdapter, ValidationError

from forze.application.contracts.querying.capabilities import UNSUPPORTED_QUERY_FEATURE_CODE
from forze.application.contracts.search import (
    FederatedSearchSpec,
    HighlightOptions,
    HubSearchSpec,
    MultiSourceSearchOptions,
    SearchCapabilities,
    SearchOptions,
    SearchSpec,
    prepare_federated_search_options,
    prepare_hub_search_options,
    reject_federated_facets,
    search_options_for_simple_adapter,
)
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #


class _M(BaseModel):
    x: str = ""


def _leg(name: str) -> SearchSpec[_M]:
    return SearchSpec(name=name, model_type=_M, fields=["x"])


def test_search_options_for_simple_adapter_strips_hub_keys() -> None:
    opts = search_options_for_simple_adapter(
        {"member_weights": {"a": 1.0}, "weights": {"x": 1.0}, "fuzzy": True},
        spec=_leg("a"),
    )
    assert "member_weights" not in opts
    assert "members" not in opts
    assert opts.get("weights") == {"x": 1.0}
    assert opts.get("fuzzy") is True


def test_a_spec_default_search_count_fills_an_absent_option() -> None:
    spec = SearchSpec(name="s", model_type=_M, fields=["x"], default_search_count="none")

    assert search_options_for_simple_adapter(None, spec=spec).get("search_count") == "none"
    # The request's own option wins over the spec default.
    asked = search_options_for_simple_adapter({"search_count": "exact"}, spec=spec)
    assert asked.get("search_count") == "exact"
    # Without a spec default the option stays absent, so the backend keeps ``exact``.
    assert "search_count" not in search_options_for_simple_adapter(None, spec=_leg("a"))
    # Without a spec (a third-party adapter's call) nothing is filled in.
    assert "search_count" not in search_options_for_simple_adapter(None)


def test_a_hub_spec_default_search_count_fills_an_absent_option() -> None:
    hub = HubSearchSpec(
        name="h",
        model_type=_M,
        members=(
            SearchSpec(name="a", model_type=_M, fields=["x"], default_search_count="exact"),
        ),
        default_search_count="none",
    )

    # The hub's default fills the option, not the member's.
    assert prepare_hub_search_options(hub, None)[0].get("search_count") == "none"
    # The request's own option wins over the hub default.
    asked, _ = prepare_hub_search_options(hub, {"search_count": "approximate"})
    assert asked.get("search_count") == "approximate"
    # Without a hub default the option stays absent, so the backend keeps ``exact``.
    plain = HubSearchSpec(name="p", model_type=_M, members=(_leg("a"),))
    assert "search_count" not in prepare_hub_search_options(plain, None)[0]


def test_prepare_hub_strips_field_tuning_and_resolves_members() -> None:
    hub = HubSearchSpec(
        name="h",
        model_type=_M,
        members=(_leg("a"), _leg("b")),
    )
    leg_opts, weights = prepare_hub_search_options(
        hub,
        {"weights": {"x": 1.0}, "members": ["a"], "fuzzy": True},
    )
    assert "weights" not in leg_opts
    assert "fields" not in leg_opts
    assert "members" not in leg_opts
    assert leg_opts.get("fuzzy") is True
    assert weights == [1.0, 0.0]


def test_prepare_hub_member_weights_out_of_range() -> None:
    hub = HubSearchSpec(
        name="h",
        model_type=_M,
        members=(_leg("a"), _leg("b")),
    )
    with pytest.raises(CoreException, match="0.0 and 1.0"):
        prepare_hub_search_options(
            hub,
            {"member_weights": {"a": 2.0, "b": 0.5}},
        )


def test_prepare_hub_default_member_weights() -> None:
    hub = HubSearchSpec(
        name="h",
        model_type=_M,
        members=(_leg("a"), _leg("b")),
        default_member_weights={"a": 0.25, "b": 0.5},
    )
    _, weights = prepare_hub_search_options(hub, None)
    assert weights == [0.25, 0.5]


def test_prepare_hub_member_weights_ignore_unknown_members() -> None:
    hub = HubSearchSpec(
        name="h",
        model_type=_M,
        members=(_leg("a"), _leg("b")),
    )
    fake_log = MagicMock()
    with patch("forze.application.contracts.search.search_options.logger", fake_log):
        _, weights = prepare_hub_search_options(
            hub,
            {"member_weights": {"a": 1.0, "ghost": 0.5, "b": 0.25}},
        )
    fake_log.warning.assert_called()
    assert weights == [1.0, 0.25]


def test_prepare_hub_member_weights_take_precedence_over_members() -> None:
    hub = HubSearchSpec(
        name="h",
        model_type=_M,
        members=(_leg("a"), _leg("b")),
    )
    _, weights = prepare_hub_search_options(
        hub,
        {"member_weights": {"a": 0.0, "b": 1.0}, "members": ["a"]},
    )
    assert weights == [0.0, 1.0]


def test_prepare_federated_strips_field_tuning_and_resolves_members() -> None:
    fed = FederatedSearchSpec(
        name="f",
        members=(_leg("a"), _leg("b")),
    )
    leg_opts, weights = prepare_federated_search_options(
        fed,
        {"weights": {"x": 1.0}, "members": ["b"], "fuzzy": True},
    )
    assert "weights" not in leg_opts
    assert "fields" not in leg_opts
    assert "members" not in leg_opts
    assert leg_opts.get("fuzzy") is True
    assert weights == [0.0, 1.0]


def test_prepare_federated_member_weights_take_precedence_over_members() -> None:
    fed = FederatedSearchSpec(
        name="f",
        members=(_leg("a"), _leg("b")),
    )
    _, weights = prepare_federated_search_options(
        fed,
        {"member_weights": {"a": 0.0, "b": 1.0}, "members": ["a"]},
    )
    assert weights == [0.0, 1.0]


def test_prepare_federated_resolves_weights_for_hub_member() -> None:
    hub = HubSearchSpec(
        name="hub",
        model_type=_M,
        members=(_leg("in_a"), _leg("in_b")),
    )
    fed = FederatedSearchSpec(
        name="f",
        members=(hub, _leg("flat")),
    )
    _, weights = prepare_federated_search_options(
        fed,
        {"members": ["hub"]},
    )
    assert weights == [1.0, 0.0]


def test_search_options_for_simple_adapter_warns_on_members_only() -> None:
    fake_log = MagicMock()
    with patch("forze.application.contracts.search.search_options.logger", fake_log):
        opts = search_options_for_simple_adapter({"members": ["a"], "fuzzy": True}, spec=_leg("a"))
    fake_log.warning.assert_called()
    assert "members" not in opts
    assert opts.get("fuzzy") is True


# ....................... #


def _hub() -> HubSearchSpec[_M]:
    return HubSearchSpec(name="hub", model_type=_M, members=[_leg("a"), _leg("b")])


def _federated() -> FederatedSearchSpec[_M]:
    return FederatedSearchSpec(name="fed", members=[_leg("a"), _leg("b")])


_NORMALIZERS = {
    "simple": lambda options: search_options_for_simple_adapter(options, spec=_leg("a")),
    "hub": lambda options: prepare_hub_search_options(_hub(), options),
    "federated": lambda options: prepare_federated_search_options(_federated(), options),
}


@pytest.mark.parametrize("normalize", _NORMALIZERS.values(), ids=list(_NORMALIZERS))
@pytest.mark.parametrize(
    ("options", "key"),
    [
        ({"fuzzzy": True}, "fuzzzy"),
        # The old spelling of the request's ``snapshot`` argument.
        ({"result_snapshot": {"mode": True}}, "result_snapshot"),
        ({"highlight": {"pre_tagg": "<b>"}}, "pre_tagg"),
    ],
    ids=["option", "result_snapshot", "highlight"],
)
def test_an_option_no_type_defines_is_refused_by_name(
    normalize: Any, options: dict[str, Any], key: str
) -> None:
    with pytest.raises(CoreException, match=key) as refused:
        normalize(options)

    assert refused.value.kind is ExceptionKind.PRECONDITION


def test_a_snapshot_key_no_type_defines_is_refused_by_name() -> None:
    snapshot: Any = {"id": "run-1", "ttl_second": 60}

    for refuse in (
        lambda: search_options_for_simple_adapter(None, spec=_leg("a"), snapshot=snapshot),
        lambda: prepare_hub_search_options(_hub(), None, snapshot=snapshot),
        lambda: prepare_federated_search_options(_federated(), None, snapshot=snapshot),
    ):
        with pytest.raises(CoreException, match="ttl_second"):
            refuse()

    # A federated search reads its options for facets first, and refuses there.
    with pytest.raises(CoreException, match="fuzzzy"):
        reject_federated_facets({"fuzzzy": True})  # type: ignore[typeddict-unknown-key]


def test_every_option_the_type_defines_is_accepted() -> None:
    options: dict[str, Any] = {
        "fuzzy": True,
        "weights": {"x": 1.0},
        "fields": ["x"],
        "phrase_combine": "all",
        "max_candidates": 10,
        "search_count": "exact",
        "facets": ["x"],
        "facet_size": 5,
        "highlight": {
            "fields": ["x"],
            "pre_tag": "<b>",
            "post_tag": "</b>",
            "fragment_size": 10,
            "max_fragments": 1,
        },
        "query_vector": [0.1, 0.2],
    }
    multi: dict[str, Any] = {
        "member_weights": {"a": 1.0},
        "members": ["a"],
        "merge_candidates": 10,
        "fusion": "rrf",
        "member_filters": {"a": {"$values": {"x": 1}}},
    }

    # The lists track the types, so a key added to either is added here too.
    assert options.keys() == SearchOptions.__optional_keys__
    assert options["highlight"].keys() == HighlightOptions.__optional_keys__
    assert options.keys() | multi.keys() == MultiSourceSearchOptions.__optional_keys__

    vector = SearchCapabilities(supports_vector=True)
    assert (
        search_options_for_simple_adapter(options, spec=_leg("a"), capabilities=vector) == options
    )
    # Multi-source search serves no caller vector (refused below).
    multi_options = {k: v for k, v in options.items() if k != "query_vector"}
    # A hub refuses a member filter (its filter applies to the hub rows); an empty one passes.
    prepare_hub_search_options(_hub(), {**multi_options, **multi, "member_filters": {}})
    prepare_federated_search_options(_federated(), {**multi_options, **multi})


def test_a_query_vector_is_refused_where_no_vector_search_serves_it() -> None:
    options: SearchOptions = {"query_vector": [0.1, 0.2]}

    for refuse in (
        lambda: search_options_for_simple_adapter(options, spec=_leg("a")),
        lambda: prepare_hub_search_options(_hub(), options),
        lambda: prepare_federated_search_options(_federated(), options),
    ):
        with pytest.raises(CoreException) as caught:
            refuse()

        assert caught.value.code == UNSUPPORTED_QUERY_FEATURE_CODE


def test_an_explicit_none_query_vector_reads_as_none() -> None:
    options: Any = {"query_vector": None, "fuzzy": True}

    assert search_options_for_simple_adapter(options) == {"fuzzy": True}

    # Hub and federated searches, which refuse a vector, take a None one as no vector too.
    prepare_hub_search_options(_hub(), options)
    prepare_federated_search_options(_federated(), options)


_VECTOR = SearchCapabilities(supports_vector=True)


def test_a_query_vector_is_normalised_to_a_list_of_floats() -> None:
    options: Any = {"query_vector": (1, 2.5, 3)}

    normalised = search_options_for_simple_adapter(
        options, capabilities=_VECTOR, vector_dimensions=3
    )

    assert normalised["query_vector"] == [1.0, 2.5, 3.0]


@pytest.mark.parametrize(
    "raw",
    [
        [10**400, 1, 2],
        [1.0, float("inf"), 2.0],
        [1.0, "x", 2.0],
        (x for x in (1.0, 2.0, 3.0)),
        {1.0, 2.0, 3.0},
        {"a": 1.0, "b": 2.0, "c": 3.0}.values(),
        "abc",
        [],
    ],
    ids=["overflow", "inf", "text", "generator", "set", "dict-view", "str", "empty"],
)
def test_a_query_vector_that_is_not_finite_numbers_is_the_callers_error(raw: Any) -> None:
    with pytest.raises(CoreException) as caught:
        search_options_for_simple_adapter({"query_vector": raw}, capabilities=_VECTOR)

    assert caught.value.kind is ExceptionKind.VALIDATION


def test_a_query_vectors_length_is_checked_before_its_values() -> None:
    unconvertible: Any = [object()] * 4

    with pytest.raises(CoreException, match="must have 3 values, got 4"):
        search_options_for_simple_adapter(
            {"query_vector": unconvertible}, capabilities=_VECTOR, vector_dimensions=3
        )

    with pytest.raises(CoreException, match="at most 16000"):
        search_options_for_simple_adapter(
            {"query_vector": [object()] * 16_001}, capabilities=_VECTOR
        )


def test_a_request_body_caps_the_query_vector_at_pgvectors_limit() -> None:
    body = TypeAdapter(SearchOptions)

    assert body.validate_python({"query_vector": [0.5] * 16_000})["query_vector"]
    assert body.validate_python({"query_vector": None})["query_vector"] is None

    with pytest.raises(ValidationError):
        body.validate_python({"query_vector": [0.5] * 16_001})
