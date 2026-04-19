"""Unit tests for search weight helpers."""

from collections.abc import Mapping

from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze_postgres.adapters.search._utils import calculate_effective_field_weights

# ----------------------- #


class _Doc(BaseModel):
    a: str
    b: str


def _spec(*, default_weights: Mapping[str, float]) -> SearchSpec[_Doc]:
    return SearchSpec(
        name="s",
        model_type=_Doc,
        fields=["a", "b"],
        default_weights=default_weights,
    )


def test_calculate_effective_field_weights_uses_fields_option() -> None:
    spec = SearchSpec(name="s", model_type=_Doc, fields=["a", "b"])
    weights = calculate_effective_field_weights(spec, {"fields": ["a"]})
    assert weights == {"a": 1.0, "b": 0.0}


def test_calculate_effective_field_weights_uses_spec_default_weights() -> None:
    spec = _spec(default_weights={"a": 0.25, "b": 0.75})
    weights = calculate_effective_field_weights(spec, {})
    assert weights == {"a": 0.25, "b": 0.75}
