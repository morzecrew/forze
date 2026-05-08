"""Unit tests for :mod:`forze_postgres.adapters.search._utils`."""

from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze.application.contracts.search import calculate_effective_field_weights


class _Doc(BaseModel):
    a: str
    b: str


def _spec(
    *,
    fields: tuple[str, ...] = ("title", "body"),
    default_weights: dict[str, float] | None = None,
) -> SearchSpec[_Doc]:
    return SearchSpec(
        name="test-doc",
        model_type=_Doc,
        fields=fields,
        default_weights=default_weights,
    )


class TestCalculateEffectiveFieldWeights:
    """Tests for :func:`calculate_effective_field_weights`."""

    def test_explicit_weights_take_priority(self) -> None:
        """When ``weights`` is set, it defines all spec fields (missing -> 0)."""
        spec = _spec(fields=("a", "b", "c"))
        out = calculate_effective_field_weights(
            spec,
            {"weights": {"a": 0.5, "b": 0.25}},
        )
        assert out == {"a": 0.5, "b": 0.25, "c": 0.0}

    def test_fields_list_without_weights(self) -> None:
        """``fields`` option assigns 1.0 to listed fields and 0.0 elsewhere."""
        spec = _spec()
        out = calculate_effective_field_weights(
            spec,
            {"fields": ["title"]},
        )
        assert out == {"title": 1.0, "body": 0.0}

    def test_default_weights_from_spec(self) -> None:
        """Uses :attr:`SearchSpec.default_weights` when options omit tuning."""
        spec = _spec(
            default_weights={"title": 0.7, "body": 0.3},
        )
        out = calculate_effective_field_weights(spec, None)
        assert out == {"title": 0.7, "body": 0.3}

    def test_fallback_uniform_weights(self) -> None:
        """With no weights, fields list, or defaults, all fields weight 1.0."""
        spec = _spec()
        out = calculate_effective_field_weights(spec, {})
        assert out == {"title": 1.0, "body": 1.0}
