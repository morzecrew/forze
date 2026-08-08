#!/usr/bin/env python3
"""Supporting tool for the gitmoji-conventional skill: validate commit messages.

Checks a message against `<gitmoji> <type>[scope][!]: <description>` — including
the emoji↔type pairing, which it reads from `references/gitmoji-mapping.md` so the
reference stays the single source of truth instead of being restated in code.

  --file PATH        validate one message file (the commit-msg hook shape)
  --message TEXT     validate a literal message
  --range REV..REV   validate every commit in a git range (audit a branch)
  (no input flag)    read the message from stdin

Checks:
  C1  subject matches the format
  C2  the emoji is an official gitmoji
  C3  the type is the one that gitmoji maps to (💥 rides any type)
  C4  💥 / `!` / `BREAKING CHANGE:` agree with each other
  C5  `BREAKING CHANGE` is uppercase and its continuation lines are indented
  C6  description is imperative-ish and unpunctuated
  C7  a body is separated from the subject by a blank line

Subject length is reported as a warning, not a failure: the skill's cap is
"<= 72 characters when possible", and a validator should not harden a rule its
skill deliberately hedged.

Merge, fixup!, squash! and revert-generated messages are skipped — git writes
those, not you.

Exit codes: 0 clean (warnings allowed) · 1 usage/IO error · 2 failures found.
Unknown flags also exit 2, from argparse itself — check stderr to tell that
apart from a failing commit message.

Install as a hook (this is the skill at rung 3 — see `ratchet-what-you-build`):

    #!/bin/sh
    exec python3 path/to/check_commit_msg.py --file "$1"
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

SUBJECT_CAP = 72
VS16 = "️"
BOOM = "💥"

SUBJECT = re.compile(
    r"^(?P<emoji>\S+)\s+(?P<type>[a-z]+)(?:\((?P<scope>[^)]*)\))?(?P<bang>!)?:\s(?P<desc>.+)$"
)
MAPPING_ROW = re.compile(r"^\|\s*(\S+)\s*\|\s*`(:[a-z0-9_+-]+:)`\s*\|[^|]*\|\s*([a-z]+)\s*\|", re.M)
SKIP_PREFIXES = ("Merge ", "Revert ", "fixup! ", "squash! ", "amend! ")
BREAKING_TOKEN = re.compile(r"^(BREAKING[ -]CHANGE):", re.M)
# Every casing, correct or not; the accepted spellings are filtered out at the
# check. Matching case-insensitively and suppressing on "a correct one exists
# somewhere" hid a malformed marker sitting beside a well-formed one.
BREAKING_MARKER = re.compile(r"^(breaking[ -]change):", re.M | re.I)
ACCEPTED_BREAKING = {"BREAKING CHANGE", "BREAKING-CHANGE"}
# Real git trailers only. "Also:" opening a final prose paragraph is not a
# trailer, and treating it as one rejected legitimate messages.
# Git trailers are `Token: value` with no spaces in the token. Hyphenated keys
# (Co-Authored-By, Helped-by, Co-developed-by) are matched by shape, so the set
# stays open, while a single capitalized prose word ("Also:") is not a trailer.
FOOTER_TOKEN = re.compile(
    r"^(?:[A-Z][A-Za-z0-9]*(?:-[A-Za-z0-9]+)+|BREAKING[ -]CHANGE|Closes|Fixes|Refs|Cc)(?::| #)"
)

NON_IMPERATIVE = {
    "added", "adds", "fixed", "fixes", "updated", "updates", "removed", "removes",
    "changed", "changes", "refactored", "refactors", "implemented", "implements",
    "created", "creates", "deleted", "deletes", "renamed", "renames", "moved",
    "moves", "improved", "improves", "bumped", "bumps",
}


def normalize_emoji(text: str) -> str:
    return text.replace(VS16, "")


def load_mapping(script_dir: Path) -> dict[str, str]:
    path = script_dir.parent / "references" / "gitmoji-mapping.md"
    if not path.is_file():
        sys.exit(f"error: mapping reference not found at {path}")
    mapping = {
        normalize_emoji(emoji): commit_type
        for emoji, _code, commit_type in MAPPING_ROW.findall(path.read_text(encoding="utf-8"))
    }
    if not mapping:
        sys.exit(f"error: no mapping rows parsed from {path} — has its table shape changed?")
    return mapping


def check_message(message: str, mapping: dict[str, str]) -> list[tuple[str, str]]:
    """Return (level, text) findings; level is 'error' (fails) or 'warn' (reported only)."""
    lines = message.rstrip().splitlines()
    if not lines or not lines[0].strip():
        return [("error", "C1: empty commit message")]
    subject = lines[0]
    if subject.startswith(SKIP_PREFIXES):
        return []

    findings: list[tuple[str, str]] = []
    problems: list[str] = []
    match = SUBJECT.match(subject)
    if not match:
        return [
            ("error", f"C1: subject does not match '<gitmoji> <type>[(scope)][!]: <description>' — {subject!r}")
        ]

    emoji = normalize_emoji(match.group("emoji"))
    commit_type = match.group("type")
    bang = bool(match.group("bang"))
    description = match.group("desc")
    # Only the final paragraph holds trailers, so a BREAKING CHANGE line in an
    # explanatory paragraph is prose, not a footer.
    footer_start = last_paragraph_start(lines)
    footer_block = "\n".join(lines[footer_start:]) if footer_start is not None else ""
    has_breaking_footer = bool(BREAKING_TOKEN.search(footer_block))

    # 💥 first: it is deliberately absent from the mapping (its row reads
    # "underlying type + !", not a concrete type), so a membership test would
    # reject every breaking commit.
    if emoji == BOOM:
        if not bang:
            problems.append("C4: 💥 marks a breaking change — the subject needs '!' before the colon")
        # 💥 carries no type of its own, but the type still has to be a real one:
        # skipping the check let any invented type through on a breaking commit,
        # the one commit class most likely to be read later.
        if commit_type not in set(mapping.values()):
            problems.append(
                f"C2: '{commit_type}' is not a conventional commit type "
                "(see references/gitmoji-mapping.md)"
            )
    elif emoji not in mapping:
        problems.append(f"C2: {match.group('emoji')} is not an official gitmoji (see references/gitmoji-mapping.md)")
    elif mapping[emoji] != commit_type:
        problems.append(
            f"C3: {match.group('emoji')} maps to '{mapping[emoji]}', but the subject says '{commit_type}'"
        )

    if (bang or has_breaking_footer) and emoji != BOOM:
        signal = "'!'" if bang else "a BREAKING CHANGE footer"
        problems.append(f"C4: {signal} marks this breaking — the gitmoji should be 💥")
    if has_breaking_footer and not bang:
        problems.append("C4: a BREAKING CHANGE footer needs '!' in the subject too")

    # Judge each marker on its own: trailer parsers read one line at a time, so a
    # correct footer elsewhere does not redeem a mis-cased one beside it.
    for marker in BREAKING_MARKER.finditer(footer_block):
        if marker.group(1) not in ACCEPTED_BREAKING:
            problems.append(f"C5: '{marker.group(1)}:' must be uppercase — 'BREAKING CHANGE:'")

    problems.extend(check_footer_folding(lines))

    if not description.strip():
        problems.append("C6: description is empty")
    if description.endswith("."):
        problems.append("C6: description ends with a period")
    words = description.split()
    first_word = words[0].lower().strip(",:") if words else ""
    if first_word in NON_IMPERATIVE:
        problems.append(f"C6: description starts with '{first_word}' — use the imperative ('add', not 'added')")
    if len(lines) > 1 and lines[1].strip():
        problems.append("C7: body must be separated from the subject by a blank line")

    findings.extend(("error", problem) for problem in problems)
    if len(subject) > SUBJECT_CAP:
        findings.append(("warn", f"C6: subject is {len(subject)} characters (aim for <= {SUBJECT_CAP})"))
    return findings


def check_footer_folding(lines: list[str]) -> list[str]:
    """A footer's continuation lines must be indented, or they detach from the token.

    Git only reads trailers from the **last** paragraph, so the scan starts there.
    Checking the whole body would misread ordinary prose that happens to open with
    a capitalized word and a colon ("Also: we changed X") as a trailer token.
    """
    problems: list[str] = []
    start = last_paragraph_start(lines)
    if start is None:
        return problems
    in_footer = False
    for number, line in enumerate(lines[start:], start=start + 1):
        if not line.strip():
            in_footer = False
            continue
        if FOOTER_TOKEN.match(line):
            in_footer = True
            continue
        if in_footer and not line.startswith((" ", "\t")):
            problems.append(
                f"C5 line {number}: continuation of the previous footer is not indented — "
                "trailer parsers will drop it"
            )
            in_footer = False
    return problems


def last_paragraph_start(lines: list[str]) -> int | None:
    """Index of the first line of the final paragraph, skipping the subject."""
    body = lines[1:]
    if not any(line.strip() for line in body):
        return None
    end = len(lines)
    while end > 1 and not lines[end - 1].strip():
        end -= 1
    index = end - 1
    while index > 1 and lines[index - 1].strip():
        index -= 1
    return index


def commits_in_range(rev_range: str) -> list[tuple[str, str]]:
    sep = "\x1e"
    proc = subprocess.run(
        ["git", "log", f"--format=%H%n%B{sep}", rev_range], capture_output=True, text=True
    )
    if proc.returncode != 0:
        sys.exit(f"error: git log {rev_range} failed: {proc.stderr.strip()[:300]}")
    commits = []
    for chunk in proc.stdout.split(sep):
        chunk = chunk.strip("\n")
        if not chunk.strip():
            continue
        sha, _, message = chunk.partition("\n")
        commits.append((sha.strip()[:8], message))
    return commits


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--file", type=Path, help="message file (commit-msg hook)")
    source.add_argument("--message", help="literal message")
    source.add_argument("--range", dest="rev_range", help="git range, e.g. main..HEAD")
    args = parser.parse_args()

    mapping = load_mapping(Path(__file__).resolve().parent)

    if args.rev_range:
        failures = warnings = 0
        commits = commits_in_range(args.rev_range)
        for sha, message in commits:
            findings = check_message(message, mapping)
            if not findings:
                continue
            errors = [text for level, text in findings if level == "error"]
            warnings += len(findings) - len(errors)
            failures += bool(errors)
            subject = message.splitlines()[0] if message.splitlines() else "(empty message)"
            print(f"{'FAIL' if errors else 'WARN'} {sha} {subject}")
            for level, text in findings:
                print(f"     {'' if level == 'error' else 'warn: '}{text}")
        print(
            f"{'FAIL ' if failures else 'OK   '} {len(commits)} commit(s), "
            f"{failures} failing, {warnings} warning(s)"
        )
        return 2 if failures else 0

    strip_editor_comments = False
    if args.file:
        if not args.file.is_file():
            sys.exit(f"error: {args.file} not found")
        message = args.file.read_text(encoding="utf-8")
        strip_editor_comments = True
    elif args.message is not None:
        message = args.message
    else:
        message = sys.stdin.read()

    # Only a commit-msg *file* carries the editor's comment lines. Stripping them
    # from a literal or stdin message would delete legitimate body lines.
    if strip_editor_comments:
        message = "\n".join(line for line in message.splitlines() if not line.startswith("#"))
    findings = check_message(message, mapping)
    errors = [text for level, text in findings if level == "error"]
    for level, text in findings:
        print(f"{'PROBLEM' if level == 'error' else 'WARNING'} {text}")
    print(f"{'FAIL ' if errors else 'OK   '} {len(errors)} problem(s), {len(findings) - len(errors)} warning(s)")
    return 2 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
