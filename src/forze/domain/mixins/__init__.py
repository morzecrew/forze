"""Reusable domain mixins for common document concerns."""

from .creator import CreatorCreateCmdMixin, CreatorMixin
from .name import NameCreateCmdMixin, NameMixin, NameUpdateCmdMixin
from .number import NumberCreateCmdMixin, NumberMixin, NumberUpdateCmdMixin
from .soft_deletion import SoftDeletionMixin

# ----------------------- #

__all__ = [
    "NameMixin",
    "NameCreateCmdMixin",
    "NameUpdateCmdMixin",
    "NumberMixin",
    "NumberCreateCmdMixin",
    "NumberUpdateCmdMixin",
    "SoftDeletionMixin",
    "CreatorMixin",
    "CreatorCreateCmdMixin",
]
