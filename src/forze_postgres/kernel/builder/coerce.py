from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

import uuid
from datetime import date, datetime, timezone
from typing import Any

from forze.base.errors import ValidationError

from ..introspect import PostgresType

# ----------------------- #


def _parse_bool(v: Any) -> bool:
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

    raise ValidationError(f"Неверное значение для boolean: {v}")


# ....................... #


def _parse_uuid(v: Any) -> uuid.UUID:
    if isinstance(v, uuid.UUID):
        return v

    if isinstance(v, str):
        try:
            return uuid.UUID(v)

        except Exception as e:
            raise ValidationError(f"Invalid UUID value: {v}") from e

    raise ValidationError(f"Неверное значение для UUID: {v}")


# ....................... #


def _parse_int(v: Any, expected: str) -> int:
    if isinstance(v, bool):
        raise ValidationError(f"Ожидается {expected}, получили boolean")

    if isinstance(v, int):
        return v

    if isinstance(v, float) and v.is_integer():
        return int(v)

    if isinstance(v, str):
        s = v.strip()

        try:
            return int(s, 10)

        except Exception as e:
            raise ValidationError(f"Неверное значение для {expected}: {v}") from e

    raise ValidationError(f"Неверное значение для {expected}: {v}")


# ....................... #


def _parse_float(v: Any, expected: str) -> float:
    if isinstance(v, bool):
        raise ValidationError(f"Ожидается {expected}, получили boolean")

    if isinstance(v, (int, float)):
        return float(v)

    if isinstance(v, str):
        s = v.strip().replace(",", ".")

        try:
            return float(s)

        except Exception as e:
            raise ValidationError(f"Неверное значение для {expected}: {v}") from e

    raise ValidationError(f"Неверное значение для {expected}: {v}")


# ....................... #


def _parse_date(v: Any) -> date:
    if isinstance(v, date) and not isinstance(v, datetime):
        return v

    if isinstance(v, datetime):
        return v.date()

    if isinstance(v, (int, float)) or (
        isinstance(v, str) and v.strip().lstrip("+-").replace(".", "", 1).isdigit()
    ):
        dt = _parse_datetime(v, force_tz=True)
        return dt.date()

    if isinstance(v, str):
        s = v.strip()

        try:
            return date.fromisoformat(s)

        except Exception as e:
            raise ValidationError(f"Неверное значение для date: {v}") from e

    raise ValidationError(f"Неверное значение для date: {v}")


# ....................... #


def _to_seconds(ts: int) -> float:
    """
    Heuristic scale detection for unix timestamps:
      - seconds:      ~1e9 .. 1e10 (10 digits)
      - milliseconds: ~1e12 .. 1e13 (13 digits)
      - microseconds: ~1e15 .. 1e16 (16 digits)
      - nanoseconds:  ~1e18 .. 1e19 (19 digits)
    We'll decide by absolute magnitude.
    """

    a = abs(ts)

    # treat small numbers as seconds too
    if a < 10**11:  # < 100_000_000_000
        return float(ts)  # seconds

    if a < 10**14:
        return ts / 1_000.0  # ms

    if a < 10**17:
        return ts / 1_000_000.0  # us

    # ns (or larger) -> seconds
    return ts / 1_000_000_000.0


# ....................... #


def _parse_datetime(v: Any, *, force_tz: bool) -> datetime:
    if isinstance(v, datetime):
        dt = v

    elif isinstance(v, (int, float)) or (
        isinstance(v, str) and v.strip().lstrip("+-").replace(".", "", 1).isdigit()
    ):
        # normalize to float seconds
        if isinstance(v, str):
            s = v.strip()
            # distinguish int-like and float-like
            if "." in s:
                num = float(s)
            else:
                num = int(s)
        else:
            num = v

        if isinstance(num, float) and not num.is_integer():
            seconds = float(num)  # float w/ fraction -> seconds
        else:
            seconds = _to_seconds(int(num))

        try:
            # interpret numeric timestamp as UTC epoch
            dt = datetime.fromtimestamp(seconds, tz=timezone.utc)

        except Exception as e:
            raise ValidationError(f"Неверное значение для datetime: {v}") from e

    elif isinstance(v, str):
        s = v.strip()

        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))

        except Exception as e:
            raise ValidationError(f"Неверное значение для datetime: {v}") from e

    else:
        raise ValidationError(f"Неверное значение для datetime: {v}")

    if force_tz:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

    else:
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

    return dt


# ....................... #


def coerce_value(v: Any, t: PostgresType) -> Any:
    """
    Coerce a scalar value to match expected PG type.

    Returns Python-native values that psycopg адаптирует корректно:
    - uuid.UUID
    - datetime/date
    - bool/int/float/str
    """

    if v is None:
        if t.not_null:
            raise ValidationError(
                f"NULL не разрешен для не-NULL столбца: {t.base + ('[]' if t.is_array else '')}"
            )
        return None

    base = t.base

    # NOTE: arrays should be handled outside (see coerce_seq)
    if t.is_array:
        raise ValidationError(f"Ожидается массив (list/tuple), получили {type(v)}")

    if base == "uuid":
        return _parse_uuid(v)

    if base in {"text", "varchar", "char", "citext"}:
        # оставляем как есть, но приводим числа к строке для удобства
        return v if isinstance(v, str) else str(v)

    if base in {"bool"}:
        return _parse_bool(v)

    if base in {"int2", "int4", "int8"}:
        return _parse_int(v, base)

    if base in {"float4", "float8", "numeric"}:
        return _parse_float(v, base)

    if base == "date":
        return _parse_date(v)

    if base == "timestamptz":
        return _parse_datetime(v, force_tz=True)

    if base == "timestamp":
        return _parse_datetime(v, force_tz=False)

    # fallback: не знаем тип — отдадим как есть (или можно error)
    return v


# ....................... #


def coerce_seq(v: Any, t: PostgresType) -> list[Any]:
    """
    Coerce an array value according to t.base element type.
    """
    if v is None:
        if t.not_null:
            raise ValidationError(
                f"NULL is not разрешен для не-NULL столбца: {t.base + '[]'}"
            )
        return []

    if not isinstance(v, (list, tuple)):
        raise ValidationError(f"Ожидается list/tuple, получили {type(v)}")

    elem_t = PostgresType(base=t.base, is_array=False, not_null=True)
    return [
        coerce_value(x, elem_t) for x in v  # pyright: ignore[reportUnknownVariableType]
    ]
