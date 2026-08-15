"""Gitmoji excerpt guard: the convenience table in ``CONTRIBUTING.md`` cannot contradict the skill.

``CONTRIBUTING.md`` delegates the emoji→type mapping to ``gitmoji-conventional`` and then keeps
a short excerpt of it, calling itself "a strict subset with identical types — **not** a competing
mapping". That sentence is a promise about two files, and a promise about two files is what drifts.

It already did. The table this excerpt replaced carried six rows whose subjects the skill's own
validator rejects, and 15 of the 120 commits before it was fixed fail on exactly those rows — so
the excerpt did not merely go stale, it actively taught a convention the gate refuses. That is the
worst shape of drift, because the reader who follows the local copy is the one who gets blocked.

The guard is therefore not "are these two files similar". It is the excerpt's own claim, checked:
every row it lists exists upstream, with the same type. Rows the excerpt omits are the point of an
excerpt and are never a finding; a row it *invents*, or one whose type disagrees, is.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

# ----------------------- #

_REPO = Path(__file__).resolve().parents[2]
_SKILL_MAPPING = (
    _REPO / ".claude" / "skills" / "gitmoji-conventional" / "references" / "gitmoji-mapping.md"
)
_CONTRIBUTING = _REPO / "CONTRIBUTING.md"

_EXCERPT_HEADING = "### Most used in this repository"
"""The section whose table is the excerpt. Anchoring here keeps the guard off every other table
in the file — a match-any-table parser would silently start grading the conventions table too."""


def _normalize_emoji(text: str) -> str:
    """Drop the variation selector, so ``🗃`` and ``🗃️`` are one glyph.

    The skill's validator normalizes the same way; a guard that did not would fail on a
    difference the tool it guards deliberately ignores.
    """

    return text.replace("️", "").strip()


def _normalize_type(text: str) -> str:
    """Strip markdown emphasis and code ticks, so ``*underlying type* + `!` `` == ``underlying
    type + `!` ``. The two files format that one cell differently and mean the same thing."""

    return re.sub(r"[*`_]", "", text).strip()


def _table_rows(lines: list[str]) -> dict[str, str]:
    """Emoji → type for every table row in *lines*, splitting multi-emoji cells (``➕ ➖``)."""

    rows: dict[str, str] = {}

    for line in lines:
        if not line.startswith("|"):
            continue

        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]

        if len(cells) < 2 or cells[0] in ("Gitmoji", "") or set(cells[0]) <= {"-", ":"}:
            continue

        # The skill's table is | emoji | code | meaning | type |; the excerpt's is
        # | emoji | type | use for |. The type is the last cell upstream, the second here.
        type_cell = cells[-1] if len(cells) == 4 else cells[1]

        for glyph in cells[0].split():
            emoji = _normalize_emoji(glyph)
            if emoji:
                rows[emoji] = _normalize_type(type_cell)

    return rows


def _skill_mapping() -> dict[str, str]:
    return _table_rows(_SKILL_MAPPING.read_text(encoding="utf-8").splitlines())


def _contributing_excerpt() -> dict[str, str]:
    lines = _CONTRIBUTING.read_text(encoding="utf-8").splitlines()

    try:
        start = lines.index(_EXCERPT_HEADING)
    except ValueError:  # pragma: no cover - covered by its own test below
        pytest.fail(
            f"{_CONTRIBUTING.name} has no {_EXCERPT_HEADING!r} section. If the excerpt was "
            "removed, delete this guard; if it was renamed, update _EXCERPT_HEADING — do not "
            "leave the guard pointing at a section that no longer exists.",
        )

    rest = lines[start + 1 :]
    end = next((i for i, line in enumerate(rest) if line.startswith("### ")), len(rest))

    return _table_rows(rest[:end])


# ....................... #


def test_both_tables_are_actually_parsed() -> None:
    """The empty case, decided rather than inherited.

    Every other assertion here is a subset check, and the empty set is a subset of everything.
    A parser that silently stopped matching — a reformatted table, a renamed heading, a moved
    file — would turn this guard into a permanent pass reporting on nothing. Refuse first.
    """

    skill = _skill_mapping()
    excerpt = _contributing_excerpt()

    assert len(skill) > 50, f"parsed only {len(skill)} rows from {_SKILL_MAPPING.name}"
    assert len(excerpt) > 20, f"parsed only {len(excerpt)} rows from the {_EXCERPT_HEADING!r} table"
    assert len(excerpt) < len(skill), "the excerpt is no longer an excerpt — it lists every row"


def test_the_excerpt_invents_no_gitmoji() -> None:
    """A row with no upstream entry is a local convention wearing the skill's name."""

    skill = _skill_mapping()
    invented = sorted(emoji for emoji in _contributing_excerpt() if emoji not in skill)

    assert not invented, (
        f"{_CONTRIBUTING.name} lists {invented}, which the skill's mapping does not. The excerpt "
        f"claims to be a strict subset — add the row upstream in {_SKILL_MAPPING.name} first, or "
        "drop it here."
    )


def test_every_excerpt_row_agrees_with_the_skill_on_type() -> None:
    """The failure that actually bit: a row present in both, disagreeing on the type.

    This is the one the reader cannot detect. An invented emoji at least looks unfamiliar; a
    familiar emoji filed under the wrong type reads as correct right up until the validator
    rejects the subject it produced.
    """

    skill = _skill_mapping()
    disagreements = {
        emoji: (declared, skill[emoji])
        for emoji, declared in _contributing_excerpt().items()
        if emoji in skill and declared != skill[emoji]
    }

    assert not disagreements, (
        "the excerpt disagrees with the skill on "
        + ", ".join(
            f"{emoji} (CONTRIBUTING says {ours!r}, skill says {theirs!r})"
            for emoji, (ours, theirs) in sorted(disagreements.items())
        )
    )
