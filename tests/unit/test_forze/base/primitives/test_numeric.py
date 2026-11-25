import pytest

from pydantic import BaseModel

from forze.base.primitives.numeric import Int64, IntWidth, clamp, int_width


class TestClamp:
    def test_within_range_passthrough(self) -> None:
        assert clamp(15, 10, 20) == 15

    def test_below_range_returns_lo(self) -> None:
        assert clamp(3, 10, 20) == 10

    def test_above_range_returns_hi(self) -> None:
        assert clamp(99, 10, 20) == 20

    def test_at_lower_bound(self) -> None:
        assert clamp(10, 10, 20) == 10

    def test_at_upper_bound(self) -> None:
        assert clamp(20, 10, 20) == 20

    def test_degenerate_range_single_value(self) -> None:
        assert clamp(5, 7, 7) == 7

    def test_float_within_range(self) -> None:
        assert clamp(0.8, 0.5, 1.0) == 0.8

    def test_float_clamped_to_bounds(self) -> None:
        assert clamp(2.0, 0.5, 1.0) == 1.0
        assert clamp(0.1, 0.5, 1.0) == 0.5

    def test_inverted_bounds_raise(self) -> None:
        with pytest.raises(ValueError, match="inverted"):
            clamp(5, 20, 10)


class TestIntWidth:
    def test_int64_validates_as_an_int_and_states_its_width(self) -> None:
        class Row(BaseModel):
            hlc: Int64
            size: Int64 | None = None
            count: int = 0

        fields = Row.model_fields

        assert Row(hlc=2**62).hlc == 2**62
        assert int_width(fields["hlc"].annotation, fields["hlc"].metadata) == 64
        # Inside a union the Annotated stays in the annotation, which is read through it.
        assert fields["size"].metadata == []
        assert int_width(fields["size"].annotation, fields["size"].metadata) == 64
        assert int_width(Int64 | None) == 64
        assert int_width(int | str) is None
        assert int_width(Int64) == 64
        assert int_width(fields["count"].annotation, fields["count"].metadata) is None

    def test_a_width_is_read_from_metadata_alone(self) -> None:
        assert int_width(int, [object(), IntWidth(32)]) == 32
