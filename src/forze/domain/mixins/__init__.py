"""Reusable domain mixins for common document concerns."""

from .name import NameCreateCmdMixin, NameMixin, NameUpdateCmdMixin
from .number import NumberCreateCmdMixin, NumberMixin, NumberUpdateCmdMixin
from .soft_deletion import (
    SoftDeletionCreateCmdMixin,
    SoftDeletionMixin,
    SoftDeletionUpdateCmdMixin,
)

# ----------------------- #

__all__ = [
    "NameMixin",
    "NameCreateCmdMixin",
    "NameUpdateCmdMixin",
    "NumberMixin",
    "NumberCreateCmdMixin",
    "NumberUpdateCmdMixin",
    "SoftDeletionMixin",
    "SoftDeletionCreateCmdMixin",
    "SoftDeletionUpdateCmdMixin",
]
