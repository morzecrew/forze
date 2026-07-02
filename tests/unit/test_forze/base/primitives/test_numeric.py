import pytest

from forze.base.primitives.numeric import clamp


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

    def test_inverted_bounds_raise(self) -> None:
        with pytest.raises(ValueError, match="inverted"):
            clamp(5, 20, 10)
