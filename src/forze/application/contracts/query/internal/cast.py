from datetime import date, datetime, timezone
from typing import Any
from uuid import UUID

from ..types import Scalar

# ----------------------- #


class QueryValueCaster:
    """Static methods for casting raw values to typed scalars.

    Used when rendering filter expressions to backend-specific formats
    (e.g. MongoDB, Postgres) where values may arrive as strings or numbers.
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

        raise ValueError(f"Invalid boolean value: {v!r}")

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
                raise ValueError(f"Invalid UUID value: {v!r}") from e

        raise ValueError(f"Invalid UUID value: {v!r}")

    # ....................... #

    @staticmethod
    def as_int(v: Any) -> int:
        """Cast a value to int; rejects bool."""
        if isinstance(v, bool):
            raise ValueError("Expected int, got bool")

        if isinstance(v, int):
            return v

        if isinstance(v, float) and v.is_integer():
            return int(v)

        if isinstance(v, str):
            s = v.strip()

            try:
                return int(s, 10)

            except Exception as e:
                raise ValueError(f"Invalid int: {v!r}") from e

        raise ValueError(f"Invalid int: {v!r}")

    # ....................... #

    @staticmethod
    def as_float(v: Any) -> float:
        """Cast a value to float; rejects bool."""
        if isinstance(v, bool):
            raise ValueError("Expected float, got bool")

        if isinstance(v, (int, float)):
            return float(v)

        if isinstance(v, str):
            s = v.strip().replace(",", ".")

            try:
                return float(s)

            except Exception as e:
                raise ValueError(f"Invalid float: {v!r}") from e

        raise ValueError(f"Invalid float: {v!r}")

    # ....................... #

    @staticmethod
    def _to_seconds(v: Any) -> float:
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
    def _like_num(x: Any):
        try:
            float(x)
            return True

        except Exception:
            return False

    # ....................... #

    @classmethod
    def as_datetime(cls, v: Any, *, force_tz: bool) -> datetime:
        """Cast a value to datetime; accepts ISO string or timestamp."""
        if isinstance(v, datetime):
            dt = v

        elif cls._like_num(v):
            if isinstance(v, str):
                s = v.strip()
                num = float(s) if "." in s else int(s)

            else:
                num = v

            seconds = (
                float(num)
                if isinstance(num, float) and not num.is_integer()
                else cls._to_seconds(int(num))
            )

            try:
                dt = datetime.fromtimestamp(seconds, tz=timezone.utc)

            except Exception as e:
                raise ValueError(f"Invalid datetime timestamp: {v!r}") from e

        elif isinstance(v, str):
            s = v.strip()

            try:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))

            except Exception as e:
                raise ValueError(f"Invalid datetime: {v!r}") from e

        else:
            raise ValueError(f"Invalid datetime: {v!r}")

        if force_tz:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

        else:
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

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
                raise ValueError(f"Invalid date: {v!r}") from e

        raise ValueError(f"Invalid date: {v!r}")

    # ....................... #

    @staticmethod
    def pass_through(v: Any) -> Any:
        """Return scalar as-is; coerce other values to string."""
        if v is None or isinstance(v, Scalar):
            return v

        return str(v)
