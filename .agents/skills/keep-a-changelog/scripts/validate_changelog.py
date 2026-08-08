#!/usr/bin/env python3
"""Supporting tool for the keep-a-changelog skill: validate CHANGELOG.md structure.

Checks the [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/) rules a
machine can settle, so review attention goes to what only a human can judge —
whether an entry is user-relevant, outcome-oriented, and true.

Spec checks (always on):
  S1  an `## [Unreleased]` section exists
  S2  every other `##` heading is `## [X.Y.Z] - YYYY-MM-DD` (optionally ` [YANKED]`)
  S3  dates are real ISO 8601 calendar dates
  S4  versions run latest-first
  S5  `###` headings are only the six spec categories
  S6  no duplicate version sections
  S7  link references resolve both ways — skipped entirely when the file uses none

House rules (`--house-rules`, this repository's local conventions):
  H1  a blank line between bullet entries
  H2  at most 320 characters per entry
  H3  at most 3 sentences per entry

Exit codes: 0 clean · 1 usage/IO error · 2 problems found. Unknown flags exit 2,
from argparse itself — check stderr to tell that apart from a failing changelog.

What belongs in the changelog at all, and how an entry is worded, stay in
SKILL.md — this tool never edits, only reports.
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
from pathlib import Path

CATEGORIES = ("Added", "Changed", "Deprecated", "Removed", "Fixed", "Security")
MAX_CHARS = 320
MAX_SENTENCES = 3

UNRELEASED = re.compile(r"^##\s+\[Unreleased\]\s*$", re.I)
VERSION_HEADING = re.compile(
    r"^##\s+\[(?P<version>[^\]]+)\]\s+-\s+(?P<date>\S+)\s*(?P<yanked>\[YANKED\])?\s*$"
)
ANY_H2 = re.compile(r"^##\s+(.*)$")
H3 = re.compile(r"^###\s+(.*)$")
LINK_DEF = re.compile(r"^\[([^\]]+)\]:\s*\S+", re.M)
BULLET = re.compile(r"^-\s+(.*)$")
SEMVER_CORE = re.compile(r"^(\d+)\.(\d+)\.(\d+)")
SENTENCE_END = re.compile(r"[.!?](?:\s|$)")
# GFM: a fence is indented at most three spaces, and a backtick fence's info
# string may not itself contain a backtick. Lines that break either rule are
# ordinary content, and treating them as delimiters skips real structure.
FENCE = re.compile(r"^ {0,3}(`{3,}(?!.*`)|~{3,})[ \t]*(\S.*)?$")
VERSION = re.compile(r"^v?\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.\-]+)?$")
ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def outside_fences(lines: list[str], start: int, end: int) -> list[int]:
    """Line numbers in [start, end) that are not inside a fenced block.

    A changelog that documents its own format contains ``### Added`` and
    bullets inside examples; scanning those reports the document's own
    illustrations as violations.
    """
    kept: list[int] = []
    opener: tuple[str, int] | None = None
    for number in range(start, min(end, len(lines))):
        match = FENCE.match(lines[number])
        if match:
            run, trailing = match.group(1), match.group(2)
            char, length = run[0], len(run)
            if opener is None:
                # An opening fence may carry an info string; a closing one may not.
                opener = (char, length)
                continue
            # GFM closes a block only with the same character, a run at least as
            # long as the opener, and nothing but whitespace after it.
            if char == opener[0] and length >= opener[1] and not trailing:
                opener = None
                continue
        if opener is None:
            kept.append(number)
    return kept


def core_version(version: str) -> tuple[int, int, int] | None:
    match = SEMVER_CORE.match(version.strip().lstrip("vV"))
    return (int(match.group(1)), int(match.group(2)), int(match.group(3))) if match else None


def entry_texts(lines: list[str], live: list[int]) -> list[tuple[int, str]]:
    """Bullet entries among `live` line numbers, folded with continuation lines."""
    entries: list[tuple[int, str]] = []
    live_set = set(live)
    position = 0
    while position < len(live):
        index = live[position]
        match = BULLET.match(lines[index])
        if not match:
            position += 1
            continue
        first_line = index
        text = match.group(1).strip()
        position += 1
        while position < len(live):
            following = live[position]
            if following not in live_set or not lines[following].strip() or BULLET.match(lines[following]):
                break
            if lines[following].startswith(("  ", "\t")):
                text += " " + lines[following].strip()
            position += 1
        entries.append((first_line, text))
    return entries


def validate(path: Path, house_rules: bool) -> list[str]:
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    problems: list[str] = []

    sections: list[dict] = []
    live_lines = set(outside_fences(lines, 0, len(lines)))
    for number, line in enumerate(lines):
        if number not in live_lines:
            continue
        heading = ANY_H2.match(line)
        if not heading:
            continue
        if UNRELEASED.match(line):
            sections.append({"kind": "unreleased", "line": number, "version": None})
            continue
        version_match = VERSION_HEADING.match(line)
        if version_match:
            sections.append(
                {
                    "kind": "version",
                    "line": number,
                    "version": version_match.group("version"),
                    "date": version_match.group("date"),
                    "yanked": bool(version_match.group("yanked")),
                }
            )
        else:
            # Non-version H2s (an intro "Changelog" title lives at H1) are suspect.
            problems.append(
                f"S2 line {number + 1}: '## {heading.group(1).strip()}' is not "
                "'## [Unreleased]' or '## [X.Y.Z] - YYYY-MM-DD'"
            )

    unreleased = [s for s in sections if s["kind"] == "unreleased"]
    if not unreleased:
        problems.append("S1: no '## [Unreleased]' section")
    elif len(unreleased) > 1:
        problems.append(
            f"S1: {len(unreleased)} '## [Unreleased]' sections (lines "
            f"{', '.join(str(s['line'] + 1) for s in unreleased)}) — the spec has exactly one"
        )
    elif sections and sections[0] is not unreleased[0]:
        problems.append(
            f"S1 line {unreleased[0]['line'] + 1}: '## [Unreleased]' must be the first section"
        )

    versions = [s for s in sections if s["kind"] == "version"]
    for section in versions:
        # fromisoformat also accepts compact forms like 20260101, which the
        # spec's YYYY-MM-DD requirement does not.
        valid_shape = bool(ISO_DATE.match(section["date"]))
        try:
            dt.date.fromisoformat(section["date"])
        except ValueError:
            valid_shape = False
        if not valid_shape:
            problems.append(
                f"S3 line {section['line'] + 1}: '{section['date']}' is not an ISO 8601 date (YYYY-MM-DD)"
            )

    for section in versions:
        if not VERSION.match(section["version"].strip()):
            problems.append(
                f"S2 line {section['line'] + 1}: '[{section['version']}]' is not an X.Y.Z version"
            )

    seen: dict[str, int] = {}
    for section in versions:
        if section["version"] in seen:
            problems.append(
                f"S6 line {section['line'] + 1}: version [{section['version']}] already appears "
                f"at line {seen[section['version']] + 1}"
            )
        else:
            seen[section["version"]] = section["line"]

    for earlier, later in zip(versions, versions[1:]):
        top, below = core_version(earlier["version"]), core_version(later["version"])
        if top and below and top < below:
            problems.append(
                f"S4 line {later['line'] + 1}: [{later['version']}] appears below "
                f"[{earlier['version']}] but is newer — sections run latest-first"
            )

    bounds = [s["line"] for s in sections] + [len(lines)]
    for index, section in enumerate(sections):
        start, end = section["line"] + 1, bounds[index + 1]
        live = outside_fences(lines, start, end)
        for number in live:
            category = H3.match(lines[number])
            if category and category.group(1).strip() not in CATEGORIES:
                problems.append(
                    f"S5 line {number + 1}: '### {category.group(1).strip()}' is not one of the six "
                    f"spec categories ({', '.join(CATEGORIES)})"
                )
        if house_rules:
            problems.extend(check_house_rules(lines, live))

    # Link definitions inside a fenced example are illustrations, not real ones.
    unfenced = "\n".join(lines[number] for number in outside_fences(lines, 0, len(lines)))
    defined = {name.lower() for name in LINK_DEF.findall(unfenced)}
    if defined:
        wanted = {"unreleased"} | {s["version"].lower() for s in versions}
        for missing in sorted(wanted - defined):
            problems.append(f"S7: heading [{missing}] has no link reference definition")
        for extra in sorted(defined - wanted):
            problems.append(f"S7: link reference [{extra}] matches no heading")

    return problems


def check_house_rules(lines: list[str], live: list[int]) -> list[str]:
    problems: list[str] = []
    live_set = set(live)
    for number in live:
        following = number + 1
        if following not in live_set or following >= len(lines):
            continue
        if BULLET.match(lines[number]) and BULLET.match(lines[following]):
            problems.append(f"H1 line {following + 1}: bullet stacked on the previous one — blank line between entries")
    for number, entry in entry_texts(lines, live):
        if entry == "...":
            continue
        if len(entry) > MAX_CHARS:
            problems.append(f"H2 line {number + 1}: entry is {len(entry)} characters (max {MAX_CHARS})")
        sentences = len([s for s in SENTENCE_END.split(entry) if s.strip()])
        if sentences > MAX_SENTENCES:
            problems.append(f"H3 line {number + 1}: entry has {sentences} sentences (max {MAX_SENTENCES})")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("path", nargs="?", type=Path, default=Path("CHANGELOG.md"))
    parser.add_argument(
        "--house-rules", action="store_true", help="also enforce this repository's local entry conventions"
    )
    args = parser.parse_args()
    if not args.path.is_file():
        sys.exit(f"error: {args.path} not found")

    problems = validate(args.path, args.house_rules)
    for problem in problems:
        print(f"PROBLEM {problem}")
    scope = "spec + house rules" if args.house_rules else "spec"
    print(f"{'FAIL ' if problems else 'OK   '} {args.path} ({scope}): {len(problems)} problem(s)")
    return 2 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
