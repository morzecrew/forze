import math
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from forze.base.exceptions import exc

from ..types import Scalar

# ----------------------- #

_INT8_MIN = -(1 << 63)
_INT8_MAX = (1 << 63) - 1
"""The widest integer column's range: a bound outside it matches no stored integer."""

_TIMESTAMP_BOUND = Decimal("1E+21")
"""Past any epoch timestamp in nanoseconds a ``datetime`` holds."""

# ....................... #


class QueryValueCaster:
    """Static methods for casting raw values to typed scalars.

    Used when rendering filter expressions to backend-specific formats
    where values may arrive as strings or numbers.
    """

    @staticmethod
    def as_bool(v: Any) -> bool:
        """Cast a value to bool; accepts ``"true"``, ``"1"``, etc."""

        if isinstance(v, bool):
            return v

        if isinstance(v, int) and v in (0, 1):
            return bool(v)

        if isinstance(v, str):
            s = v.strip().lower()

            if s in {"true", "t", "1", "yes", "y", "on"}:
                return True

            if s in {"false", "f", "0", "no", "n", "off"}:
                return False

        raise exc.precondition(f"Invalid boolean value (got {type(v).__name__})")

    # ....................... #

    @staticmethod
    def as_uuid(v: Any) -> UUID:
        """Cast a value to UUID; accepts UUID or parseable string."""
        if isinstance(v, UUID):
            return v

        if isinstance(v, str):
            try:
                return UUID(v)

            except Exception as e:
                raise exc.precondition(f"Invalid UUID value (got {type(v).__name__})") from e

        raise exc.precondition(f"Invalid UUID value (got {type(v).__name__})")

    # ....................... #

    @staticmethod
    def as_int(v: Any) -> int:
        """Cast a value to int; rejects bool."""
        if isinstance(v, bool):
            raise exc.precondition("Expected int, got bool")

        if isinstance(v, int):
            return v

        if isinstance(v, float) and v.is_integer():
            return int(v)

        # A string bound reaches here as the exact Decimal the shared cast made of it. It is
        # held to the widest integer column before ``int()`` expands it: ``1E+1000000`` is
        # ten characters and a million digits.
        if (
            isinstance(v, Decimal)
            and v.is_finite()
            and _INT8_MIN <= v <= _INT8_MAX
            and v == v.to_integral_value()
        ):
            return int(v)

        if isinstance(v, str):
            s = v.strip()

            try:
                return int(s, 10)

            except Exception as e:
                raise exc.precondition(f"Invalid int (got {type(v).__name__})") from e

        raise exc.precondition(f"Invalid int (got {type(v).__name__})")

    # ....................... #

    @staticmethod
    def as_float(v: Any) -> float:
        """Cast a value to float; rejects bool and non-finite values (NaN, ±Infinity)."""
        if isinstance(v, bool):
            raise exc.precondition("Expected float, got bool")

        if isinstance(v, (int, float, Decimal)):
            result = float(v)

        elif isinstance(v, str):
            # No locale guessing: "1,234" is ambiguous (thousands separator vs decimal
            # comma), and silently reading it as 1.234 is a factor-1000 error that raises
            # nothing. A comma string is refused; the caller sends the canonical form.
            s = v.strip()

            try:
                result = float(s)

            except Exception as e:
                raise exc.precondition(f"Invalid float (got {type(v).__name__})") from e

        else:
            raise exc.precondition(f"Invalid float (got {type(v).__name__})")

        # Finite only (see as_decimal): NaN/Infinity parse, but as a filter operand
        # they compare differently on every backend — fail-open, not just wrong.
        if not math.isfinite(result):
            raise exc.precondition(f"Non-finite float not allowed (got {type(v).__name__})")

        return result

    # ....................... #

    @staticmethod
    def as_decimal(v: Any) -> Decimal:
        """Cast a value to Decimal without going through binary float; rejects bool
        and non-finite values (NaN, sNaN, ±Infinity)."""
        if isinstance(v, bool):
            raise exc.precondition("Expected numeric, got bool")

        if isinstance(v, Decimal):
            result = v

        elif isinstance(v, int):
            result = Decimal(v)

        elif isinstance(v, float):
            result = Decimal(str(v))

        elif isinstance(v, str):
            # No locale guessing (see as_float): a thousands-separator comma read as a
            # decimal point turns 1,234 into 1.234 with nothing raised. Refused instead.
            s = v.strip()

            try:
                result = Decimal(s)

            except Exception as e:
                raise exc.precondition(f"Invalid numeric (got {type(v).__name__})") from e

        else:
            raise exc.precondition(f"Invalid numeric (got {type(v).__name__})")

        # ``Decimal`` parses "NaN"/"Infinity", but a non-finite value is not a range
        # bound: Postgres sorts ``'NaN'::numeric`` above every number (``$lt "NaN"``
        # matches all rows — fail-open on a money filter), while the same comparison
        # in-process raises ``InvalidOperation``. Refuse it once here so every
        # backend sees the same precondition instead of diverging.
        if not result.is_finite():
            raise exc.precondition(f"Non-finite numeric not allowed (got {type(v).__name__})")

        return result

    # ....................... #

    @staticmethod
    def _to_seconds(v: Any) -> float:
        """Normalize a numeric timestamp to seconds, handling ms/µs/ns magnitudes."""

        a = abs(v)

        if a < 10**11:
            return float(v)

        if a < 10**14:
            return v / 1_000.0

        if a < 10**17:
            return v / 1_000_000.0

        return v / 1_000_000_000.0

    # ....................... #

    @staticmethod
    def _like_num(x: Any) -> bool:
        """Return ``True`` if *x* can be converted to a float."""

        try:
            float(x)
            return True

        except Exception:
            return False

    # ....................... #

    @classmethod
    def as_datetime(cls, v: Any, *, force_tz: bool) -> datetime:
        """Cast a value to datetime; accepts ISO string or timestamp."""
        dt = cls.parse_datetime(v)

        if force_tz:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)

        else:
            if dt.tzinfo is not None:
                dt = dt.astimezone(UTC).replace(tzinfo=None)

        return dt

    # ....................... #

    @classmethod
    def parse_datetime(cls, v: Any) -> datetime:
        """Cast a value to datetime as written: a datetime or ISO string keeps its offset, or
        its lack of one; a timestamp is an instant in UTC.

        :param v: A datetime, an ISO-8601 string, or a Unix timestamp in seconds (fractional
            allowed) or whole milli-, micro- or nanoseconds.
        :returns: The datetime, aware only when *v* named an offset or was a timestamp.
        :raises CoreException: ``precondition`` for anything else, a ``bool`` included.
        """
        if isinstance(v, bool):
            raise exc.precondition(f"Invalid datetime (got {type(v).__name__})")

        if isinstance(v, datetime):
            dt = v

        elif cls._like_num(v):
            if isinstance(v, str):
                s = v.strip()
                # Digits alone stay an exact int; ``1.5``, ``1e3`` and the like are floats.
                num = int(s) if s.lstrip("+-").isdigit() else float(s)

            else:
                num = v

            # A fraction is seconds; only a whole number may be milli-, micro- or nanoseconds.
            # A Decimal is bounded before ``int()`` could expand a huge exponent.
            if isinstance(num, Decimal):
                if not num.is_finite() or abs(num) >= _TIMESTAMP_BOUND:
                    raise exc.precondition(f"Invalid datetime timestamp (got {type(v).__name__})")

                fractional = num != num.to_integral_value()

            else:
                fractional = isinstance(num, float) and not num.is_integer()

            seconds = float(num) if fractional else cls._to_seconds(int(num))

            try:
                dt = datetime.fromtimestamp(seconds, tz=UTC)

            except Exception as e:
                raise exc.precondition(
                    f"Invalid datetime timestamp (got {type(v).__name__})"
                ) from e

        elif isinstance(v, str):
            s = v.strip()

            try:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))

            except Exception as e:
                raise exc.precondition(f"Invalid datetime (got {type(v).__name__})") from e

        else:
            raise exc.precondition(f"Invalid datetime (got {type(v).__name__})")

        return dt

    # ....................... #

    @classmethod
    def as_date(cls, v: Any) -> date:
        """Cast a value to date; accepts date, datetime, or ISO string."""
        if isinstance(v, date) and not isinstance(v, datetime):
            return v

        if isinstance(v, datetime):
            return v.date()

        if cls._like_num(v):
            return cls.as_datetime(v, force_tz=True).date()

        if isinstance(v, str):
            s = v.strip()

            try:
                return date.fromisoformat(s)

            except Exception as e:
                raise exc.precondition(f"Invalid date (got {type(v).__name__})") from e

        raise exc.precondition(f"Invalid date (got {type(v).__name__})")

    # ....................... #

    @staticmethod
    def pass_through(v: Any) -> Any:
        """Return scalar as-is; coerce other values to string."""

        if v is None or isinstance(v, Scalar):
            return v

        return str(v)
