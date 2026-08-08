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
# CommonMark allows a link reference definition up to three spaces of indent.
# Anchoring at column 0 read those as absent, and S7 skips itself entirely when
# it finds no definitions — so an indented set silently disabled the check.
LINK_DEF = re.compile(r"^ {0,3}\[([^\]]+)\]:\s*\S+", re.M)
BULLET = re.compile(r"^-\s+(.*)$")
SEMVER_CORE = re.compile(r"^(\d+)\.(\d+)\.(\d+)")
# A trailing period on a known abbreviation is not a sentence boundary; counting
# it as one inflated the sentence count and failed entries that obeyed H3.
# Deliberately short. "no" was here and suppressed the stop in "No." at the end
# of an ordinary sentence, undercounting for H3; an abbreviation earns its place
# only if it is far more often an abbreviation than a word.
ABBREVIATIONS = ("e.g", "i.e", "etc", "vs", "cf", "approx")
SENTENCE_END = re.compile(
    "".join(rf"(?<!\b{re.escape(word)})" for word in ABBREVIATIONS) + r"[.!?](?:\s|$)",
    re.I,
)
# GFM: a fence is indented at most three spaces, and a backtick fence's info
# string may not itself contain a backtick. Lines that break either rule are
# ordinary content, and treating them as delimiters skips real structure.
FENCE = re.compile(r"^ {0,3}(`{3,}(?!.*`)|~{3,})[ \t]*(\S.*)?$")
# The SemVer 2.0.0 grammar, not an approximation of it: numeric identifiers
# take no leading zero, and no identifier may be empty. The loose character
# class accepted 01.2.3, 1.0.0-01 and 1.0.0-rc..1, which SemVer tooling
# rejects — and core_version then compared them as though they were versions.
# Explicit ASCII: `\d` also matches Arabic-Indic and other decimal digits, so
# `1.0.0-١a` passed S2 and reached the ordering and duplicate checks.
NUM_ID = r"0|[1-9][0-9]*"
PRE_ID = rf"(?:{NUM_ID}|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
BUILD_ID = r"[0-9A-Za-z-]+"
VERSION = re.compile(
    rf"^v?(?:{NUM_ID})\.(?:{NUM_ID})\.(?:{NUM_ID})"
    rf"(?:-{PRE_ID}(?:\.{PRE_ID})*)?"
    rf"(?:\+{BUILD_ID}(?:\.{BUILD_ID})*)?$"
)
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


def core_version(version: str) -> tuple | None:
    """A SemVer precedence key, prerelease included.

    Comparing on the numeric core alone made 1.0.0-rc.1 and 1.0.0 equal, so a
    prerelease listed above its own release passed the latest-first check.
    SemVer ranks a release above any of its prereleases, and among prereleases
    ranks numeric identifiers below alphanumeric ones.
    """
    text = version.strip().lstrip("vV")
    match = SEMVER_CORE.match(text)
    if not match:
        return None
    core = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
    remainder = text[match.end():]
    if not remainder.startswith("-"):
        return core + (1, ())
    prerelease = remainder[1:].split("+", 1)[0]
    identifiers = tuple(
        (0, int(part), "") if part.isdigit() else (1, 0, part)
        for part in prerelease.split(".")
    )
    return core + (0, identifiers)


def entry_texts(lines: list[str], live: list[int]) -> list[tuple[int, str]]:
    """Bullet entries among `live` line numbers, folded with continuation lines."""
    entries: list[tuple[int, str]] = []
    position = 0
    while position < len(live):
        index = live[position]
        match = BULLET.match(lines[index])
        if not match:
            position += 1
            continue
        first_line = index
        text = match.group(1).strip()
        previous_line = index
        position += 1
        while position < len(live):
            following = live[position]
            # `following` comes from `live`, so testing it for membership in
            # live was always false. What the fold actually needs is adjacency:
            # a line separated by a fenced block is not a continuation of this
            # entry, and joining it would measure text the entry never had.
            if (
                following != previous_line + 1
                or not lines[following].strip()
                or BULLET.match(lines[following])
                # An unindented line is not a continuation, and walking past it
                # let a later indented line be folded onto this entry — text
                # the entry never had, measured against H2 and H3.
                or not lines[following].startswith(("  ", "\t"))
            ):
                break
            text += " " + lines[following].strip()
            previous_line = following
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
        # [1.0.0] and [v1.0.0] are the same release, so the optional prefix is
        # normalized away. The rest keeps its case: SemVer compares prerelease
        # identifiers case-sensitively, so 1.0.0-RC.1 and 1.0.0-rc.1 are two
        # different releases and folding them together rejected valid files.
        raw = section["version"].strip()
        key = raw[1:] if raw[:1] in {"v", "V"} else raw
        if key in seen:
            problems.append(
                f"S6 line {section['line'] + 1}: version [{section['version']}] already appears "
                f"at line {seen[key] + 1}"
            )
        else:
            seen[key] = section["line"]

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
        # Look back from each bullet rather than forward from the previous one:
        # comparing adjacent bullet lines missed an entry that ran onto a
        # continuation line, which is exactly where entries get stacked.
        previous = number - 1
        if not BULLET.match(lines[number]) or previous < 0 or previous not in live_set:
            continue
        above = lines[previous]
        if not above.strip():
            continue
        if BULLET.match(above) or above.startswith(("  ", "\t")):
            problems.append(
                f"H1 line {number + 1}: bullet stacked on the previous entry — "
                "blank line between entries"
            )
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
