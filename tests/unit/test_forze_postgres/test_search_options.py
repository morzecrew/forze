"""Unit tests for Postgres search option normalization."""

from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import HubSearchSpec, SearchSpec
from forze.base.errors import CoreError
from forze_postgres.adapters.search._options import (
    prepare_hub_search_options,
    search_options_for_simple_adapter,
)

# ----------------------- #


class _M(BaseModel):
    x: str = ""


def _leg(name: str) -> SearchSpec[_M]:
    return SearchSpec(name=name, model_type=_M, fields=["x"])


def test_search_options_for_simple_adapter_strips_hub_keys() -> None:
    opts = search_options_for_simple_adapter(
        {"member_weights": {"a": 1.0}, "weights": {"x": 1.0}, "fuzzy": True},
    )
    assert "member_weights" not in opts
    assert "members" not in opts
    assert opts.get("weights") == {"x": 1.0}
    assert opts.get("fuzzy") is True


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
    with pytest.raises(CoreError, match="0.0 and 1.0"):
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
    with patch("forze_postgres.adapters.search._options.logger", fake_log):
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


def test_search_options_for_simple_adapter_warns_on_members_only() -> None:
    fake_log = MagicMock()
    with patch("forze_postgres.adapters.search._options.logger", fake_log):
        opts = search_options_for_simple_adapter({"members": ["a"], "fuzzy": True})
    fake_log.warning.assert_called()
    assert "members" not in opts
    assert opts.get("fuzzy") is True
