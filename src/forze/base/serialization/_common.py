"""Shared internal helpers for the pydantic serialization module."""

from typing import Sequence

# ----------------------- #


def sequence_as_list[T](seq: Sequence[T]) -> list[T]:
    """Return ``seq`` as a ``list`` without copying when already a list."""

    return seq if isinstance(seq, list) else list(seq)


# ....................... #


def validate_batch_size(batch_size: int) -> None:
    """Raise ``ValueError`` when *batch_size* is below 1."""

    if batch_size < 1:
        msg = "batch_size must be >= 1"
        raise ValueError(msg)
