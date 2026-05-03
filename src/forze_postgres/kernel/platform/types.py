from typing import Literal

# ----------------------- #

RowFactory = Literal["tuple", "dict"]
"""Row format for fetch methods: ``"dict"`` for column-keyed dicts, ``"tuple"`` for sequences."""

IsolationLevel = Literal["read committed", "repeatable read", "serializable"]
"""Supported transaction isolation levels."""
