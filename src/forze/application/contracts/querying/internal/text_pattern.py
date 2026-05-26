"""Text pattern validation and LIKE-to-regex conversion for filter operators."""

import re
from typing import Sequence

from forze.base.exceptions import exc

# ----------------------- #

_REGEX_METACHAR = re.compile(r"([\\.^$|?*+()\[\]{}])")

_NESTED_QUANTIFIER = re.compile(
    r"\([^()]*[+*?][^()]*\)[+*?]" r"|\(\.\*\)[+*?]" r"|\([^)]*[+|][^)]*\)[+*?]",
)

_LARGE_REPEAT = re.compile(r"\{\d*,(\d{3,})\}")

_MAX_ALTERNATION_BRANCHES = 64

# ....................... #


def validate_text_pattern(
    op: str,
    value: str | Sequence[str],
    *,
    max_pattern_length: int,
    max_pattern_or_branches: int,
) -> tuple[str, ...]:
    """Validate a text-operator operand and return normalized pattern branches.

    :param op: ``$like``, ``$ilike``, or ``$regex``.
    :param value: Single pattern or sequence (OR semantics).
    :returns: Non-empty tuple of validated pattern strings.
    :raises exc.precondition: On empty, oversized, or unsafe patterns.
    """

    if isinstance(value, str):
        patterns: tuple[str, ...] = (value,)
    else:
        patterns = tuple(value)

    if not patterns:
        raise exc.precondition(f"{op} operand requires at least one pattern")

    if len(patterns) > max_pattern_or_branches:
        raise exc.precondition(
            f"{op} operand exceeds maximum branch count of "
            f"{max_pattern_or_branches} (got {len(patterns)})",
        )

    validated: list[str] = []

    for i, raw in enumerate(patterns):
        # extra runtime check (just in case)
        if not isinstance(raw, str):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise exc.precondition(
                f"{op} pattern at index {i} must be a string, got {raw!r}",
            )

        pattern = raw.strip()

        if not pattern:
            raise exc.precondition(f"{op} pattern at index {i} must be non-empty")

        if len(pattern) > max_pattern_length:
            raise exc.precondition(
                f"{op} pattern at index {i} exceeds maximum length of "
                f"{max_pattern_length} (got {len(pattern)})",
            )

        if op == "$regex":
            _validate_regex_safe(pattern)

        validated.append(pattern)

    return tuple(validated)


# ....................... #


def _validate_regex_safe(pattern: str) -> None:
    """Reject regex patterns with known catastrophic backtracking shapes."""

    if _NESTED_QUANTIFIER.search(pattern):
        raise exc.precondition(
            f"$regex pattern {pattern!r} uses nested quantifiers that are not allowed",
        )

    large = _LARGE_REPEAT.search(pattern)
    if large is not None:
        raise exc.precondition(
            f"$regex pattern {pattern!r} uses a repeat upper bound that is too large",
        )

    if pattern.count("|") >= _MAX_ALTERNATION_BRANCHES:
        raise exc.precondition(
            f"$regex pattern {pattern!r} has too many alternation branches",
        )


# ....................... #


def like_pattern_to_regex(pattern: str, *, case_insensitive: bool = False) -> str:
    """Convert a SQL LIKE pattern (``%``, ``_``) to a regex string.

    Literal ``%`` and ``_`` may be escaped with backslash. Other regex
    metacharacters in the pattern are escaped.
    """

    out: list[str] = []
    i = 0

    while i < len(pattern):
        ch = pattern[i]

        if ch == "\\" and i + 1 < len(pattern):
            nxt = pattern[i + 1]

            if nxt in ("%", "_", "\\"):
                out.append(_REGEX_METACHAR.sub(r"\\\1", nxt))
                i += 2

                continue

            out.append(_REGEX_METACHAR.sub(r"\\\1", ch))
            out.append(_REGEX_METACHAR.sub(r"\\\1", nxt))
            i += 2

            continue

        if ch == "%":
            out.append(".*")

        elif ch == "_":
            out.append(".")

        else:
            out.append(_REGEX_METACHAR.sub(r"\\\1", ch))

        i += 1

    body = "".join(out)
    flags = "(?i)" if case_insensitive else ""

    return f"{flags}^{body}$"
