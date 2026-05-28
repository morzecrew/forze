from typing import Literal

# ----------------------- #

RowFactory = Literal["tuple", "dict"]
"""Row format for fetch methods: ``"dict"`` for column-keyed dicts, ``"tuple"`` for sequences."""

IsolationLevel = Literal["read_committed", "repeatable_read", "serializable"]
"""Supported transaction isolation levels."""
