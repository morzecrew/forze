"""
UUIDv7 generation and conversion utilities.

Provides utilities for generating PostgreSQL-compatible UUIDv7 identifiers
and converting between UUIDs and datetime objects. Supports high-precision
timestamp extraction and generation of time-ordered UUIDs. UUIDv7 includes
timestamp information making IDs sortable by creation time.

Notes:
    Source: https://github.com/educationwarehouse/uuid7
    Fixed: milliseconds to nanoseconds conversion
    Added: hash_from_any, hex_uuid4_from_string, hex_uuid4, error capture
"""

import hashlib
import secrets
import time
from datetime import UTC, datetime, timezone
from typing import Any, Optional
from uuid import UUID
from uuid import uuid4 as uuid4_func
from zoneinfo import ZoneInfo

import orjson
from dateutil.parser import parse as dt_parse

from ..errors import CoreError

# ----------------------- #


def uuid7(
    timestamp_ms: Optional[int | float] = None,
    timestamp_ns: Optional[int | float] = None,
) -> UUID:
    """Generate a UUIDv7-style UUID with full 64-bit nanosecond timestamp precision.

    UUID Layout (128 bits):
    -------------------------------------------------------------------
    Bits  | Field           | Description
    ------|-----------------|--------------------------------------------------
    0-47  | timestamp_ms    | Unix timestamp in milliseconds (sortable)
    48-51 | version         | UUID version (0b0111 for v7)
    52-71 | sub_ms_ns       | Sub-millisecond nanoseconds (20 bits = 0-999_999)
    72-127| random          | 54 bits of randomness (variant bits embedded)
    -------------------------------------------------------------------
    Variant is stored in bits 70–71 (byte 8, bits 6–7), set to 0b10 per RFC 4122.

    Args:
        timestamp_ms (Optional[int | float]): Unix timestamp in milliseconds.
                                             If None, uses current time.
        timestamp_ns (Optional[int | float]): Unix timestamp in nanoseconds.
                                             If None, uses current time.
                                             Cannot be specified with timestamp_ms.

    Returns:
        UUID: UUIDv7 instance with embedded timestamp.

    Raises:
        ValueError: If both timestamp_ms and timestamp_ns are provided,
                   or if timestamp is negative.
    """

    if timestamp_ms is not None and timestamp_ns is not None:
        raise CoreError("Specify only one of timestamp_ms or timestamp_ns, not both.")

    if (timestamp_ms == 0 and timestamp_ns is None) or (
        timestamp_ns == 0 and timestamp_ms is None
    ):
        return UUID("00000000-0000-0000-0000-000000000000")

    if timestamp_ns is not None:
        timestamp_ns = int(timestamp_ns)
    elif timestamp_ms is not None:
        timestamp_ns = int(timestamp_ms * 1_000_000)
    else:
        timestamp_ns = time.time_ns()

    if timestamp_ns < 0:
        raise CoreError("Timestamp must be positive.")

    timestamp_ms = timestamp_ns // 1_000_000
    sub_ms_ns = timestamp_ns % 1_000_000  # 20 bits max

    uuid_int = 0
    uuid_int |= (timestamp_ms & ((1 << 48) - 1)) << 80  # Bits 0–47
    uuid_int |= 0x7 << 76  # Bits 48–51 (version 7)
    uuid_int |= (sub_ms_ns & 0xFFFFF) << 56  # Bits 52–71 (20 bits)

    random_bits = secrets.randbits(54)  # 54-bit random
    uuid_int |= random_bits

    # Set variant (RFC 4122) in bits 62–63 to 0b10
    uuid_int &= ~(0b11 << 62)
    uuid_int |= 0b10 << 62

    return UUID(int=uuid_int)


# ....................... #


def uuid7_to_datetime(
    uuid: UUID | str,
    tz: Optional[ZoneInfo | timezone] = UTC,
    high_precision: bool = False,
) -> Optional[datetime]:
    """Extract the timestamp from a UUIDv7 and return as a datetime.

    Args:
        uuid (UUID | str): A UUIDv7-compliant UUID (can be string or UUID instance).
        tz (Optional[ZoneInfo | timezone]): Desired timezone. Defaults to UTC.
                                           Use tz=None for naive datetime.
        high_precision (bool): Whether to extract sub-millisecond precision.
                              When True, microsecond precision is included.
                              Defaults to False.

    Returns:
        Optional[datetime]: Timestamp extracted from UUIDv7, or None if UUID is not version 7.
    """

    if isinstance(uuid, str):
        uuid = UUID(uuid)

    if uuid.version != 7:
        return None

    # Extract the milliseconds timestamp (bits 0-47)
    ts_bytes = uuid.bytes[:6] + b"\x00\x00"
    ms_since_epoch = int.from_bytes(ts_bytes, "big") >> 16

    if high_precision:
        # Extract the sub-millisecond nanoseconds (bits 52-71)
        # First get the integer representation
        uuid_int = uuid.int
        # Extract bits 52-71 (20 bits for sub-ms nanoseconds)
        sub_ms_ns = (uuid_int >> 56) & 0xFFFFF  # Mask with 20 bits
        # Convert to microseconds (1 microsecond = 1000 nanoseconds)
        microseconds = sub_ms_ns // 1000
        # Create timestamp with microsecond precision
        return datetime.fromtimestamp(
            ms_since_epoch / 1000 + (microseconds / 1_000_000), tz=tz
        )
    else:
        # Original behavior - millisecond precision only
        return datetime.fromtimestamp(ms_since_epoch / 1000, tz=tz)


# ....................... #


def datetime_to_uuid7(dt: datetime | str) -> UUID:
    """Generate a PostgreSQL-compatible UUIDv7 from a given datetime.

    Args:
        dt (datetime | str): The datetime to encode (can be string or datetime instance).

    Returns:
        UUID: PostgreSQL-compatible UUIDv7 with embedded timestamp.
    """

    if isinstance(dt, str):
        dt_obj = dt_parse(dt)

    else:
        dt_obj = dt

    # ``uuid7`` expects milliseconds; ``datetime.timestamp()`` returns seconds.
    # Convert seconds to integer milliseconds to preserve ordering semantics.
    timestamp_ms = int(dt_obj.timestamp() * 1_000)

    return uuid7(timestamp_ms=timestamp_ms)


# ....................... #


def _hash_from_any(val: Any) -> str:
    if isinstance(val, dict):
        b = orjson.dumps(val, option=orjson.OPT_SORT_KEYS)

    else:
        b = orjson.dumps(val)

    hex_string = hashlib.sha256(b).hexdigest()[:32]

    return hex_string


# ....................... #


def _uuid4_from_any(val: Any) -> UUID:
    return UUID(hex=_hash_from_any(val), version=4)


# ....................... #


def uuid4(val: Any | None = None) -> UUID:
    """Return a UUIDv4, optionally derived from an arbitrary value.

    When ``val`` is provided, a deterministic UUIDv4 is generated from the
    SHA-256 hash of ``val``. When omitted, a random UUIDv4 is returned.
    """

    return _uuid4_from_any(val) if val else uuid4_func()
