"""A set operand reaches every process in one order, and so does its cursor fingerprint.

A set iterates in an order seeded per process, so the parser sorts it before handing it on.
Its sort key must not tie two different values (a date and its ISO string, ``1`` and ``"1"``)
or fall back to a form that iterates a nested set in hash order: either way a worker could
order the operand, and fingerprint the filter, differently from the worker that minted a
cursor, which would then be refused.
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

pytestmark = pytest.mark.unit

_SCRIPT = """
from datetime import date
from forze.application.contracts.querying import QueryFilterExpressionParser
from forze.application.contracts.querying.pagination.cursor_token import fingerprint_filter

operand = {
    date(2024, 1, 1), "2024-01-01", 1, "1", 2, "b", "a",
    frozenset({"x", "y", "z", "w"}), frozenset({1, "1"}), frozenset({date(2024, 1, 1), "2024-01-01"}),
}
parsed = QueryFilterExpressionParser.parse({"$values": {"f": {"$in": operand}}})
# A frozenset's own repr iterates in hash order; print each one sorted, so the line shows
# the order the parser chose, not the order Python prints a set in.
print([sorted(map(repr, v)) if isinstance(v, frozenset) else repr(v) for v in parsed.items[0].value])
print(fingerprint_filter(parsed))
"""


def _run(seed: str) -> str:
    env = {**os.environ, "PYTHONHASHSEED": seed}
    result = subprocess.run(
        [sys.executable, "-c", _SCRIPT], env=env, capture_output=True, text=True, check=True
    )

    return result.stdout


def test_the_order_and_the_fingerprint_hold_across_hash_seeds() -> None:
    outputs = {_run(seed) for seed in ("0", "1", "42", "99", "12345")}

    assert len(outputs) == 1, outputs
