#!/usr/bin/env python3
"""Supporting tool for the flag-dont-flip skill: check a task's divergence log.

Reads `logs/<task-id>.md`, finds every ```divergence block in it, and answers
the questions a reviewer would otherwise have to answer by hand:

  schema     every required field present, every vocabulary word known, `at` a
             UTC RFC 3339 stamp, `attempt` a positive integer; an entry carries
             `kind` (what happened to the decision) or `class` (what it says
             about the design process) or both — the axes are orthogonal
  legality   the action is the one the grade licenses — checked both ways, so
             halting on ASSUMED fails as loudly as departing from LOCKED; a
             `kind: resolved` close-out is exempt, because it attests
             compliance rather than reporting a contradiction
  drift      the declared `Drift count: N` equals the entries classed `drift`
  evidence   every citation resolves: a real file under --root, a line or range
             inside it, or a backticked command carrying its output
  silence    with --task and --base, every LOCKED decision whose declared paths
             the diff touched has an entry

The silence check is the one that pays. Violating a lock is not mechanically
detectable; the *absence of an entry* in an area a lock governs is trivially so.

A decision that declares no `paths` is reported as SKIPPED, never as passing.
The distinction is the whole point: a check that guesses an area produces false
silences and false clean runs, and only one of those is visible.

What this deliberately does NOT do is compare an entry's `grade` to the spec's
current table. The log records the grade that was in force when the executor
acted, and a checker that re-resolved it at read time would rewrite the log's
own past every time someone re-graded a row.

Task files are JSON, or YAML in a restricted subset (see references/). The
subset refuses what it cannot parse rather than skipping it — a task file that
was half-understood is a silence check that passes for the wrong reason.

Exit codes: 0 clean · 1 usage/IO error · 2 problems found. Unknown flags exit 2,
from argparse itself — check stderr to tell that apart from a failing log.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from fnmatch import fnmatch
from pathlib import Path

BLOCK = re.compile(r"^```divergence[ \t]*$\n(.*?)^```[ \t]*$", re.M | re.S)
OPENING_FENCE = re.compile(r"^```divergence[ \t]*$", re.M)
FIELD = re.compile(r"^([a-z_]+):[ \t]*(.*)$")
DRIFT_COUNT = re.compile(r"^\*\*Drift count:\s*(\d+)", re.M)
# UTC only. A local stamp with an offset is still RFC 3339, but two entries
# written in different time zones then sort wrongly against each other, and the
# log's whole use is ordering what happened against what was decided.
RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(?:Z|\+00:00)$")
# path:line or path:start-end, with the path stopping at the first colon that a
# line number follows — a Windows-style drive letter is not a supported path.
CITATION = re.compile(r"^(?P<path>[^\s:][^:]*):(?P<start>\d+)(?:-(?P<end>\d+))?$")
BACKTICKED = re.compile(r"^`(?P<command>[^`]+)`(?P<rest>.*)$", re.S)

REQUIRED = ("decision", "grade", "at", "attempt", "claim", "evidence", "action")
GRADES = {"LOCKED", "ASSUMED", "OPEN", "UNLISTED"}
KINDS = {"contradicted", "departed", "resolved", "blocked"}
CLASSES = {"discovery", "spec-gap", "drift", "irreducible"}
ACTIONS = {"halted", "departed", "decided"}
# The one table the skill and this script must agree on. Read in both
# directions: a grade licenses exactly one action, and an action is licensed by
# exactly the grades that map to it. A downstream gate may mirror this table
# and the kind exemptions below; two implementations of one contract must not
# diverge, so change them together.
LEGAL = {"LOCKED": "halted", "ASSUMED": "departed", "OPEN": "decided", "UNLISTED": "decided"}


class Problem:
    def __init__(self, code: str, where: str, message: str) -> None:
        self.code, self.where, self.message = code, where, message

    def __str__(self) -> str:
        return f"{self.code} {self.where}: {self.message}"


class Skipped:
    def __init__(self, what: str, why: str) -> None:
        self.what, self.why = what, why

    def __str__(self) -> str:
        return f"SKIPPED {self.what}: {self.why}"


# --------------------------------------------------------------------------- #
# parsing
# --------------------------------------------------------------------------- #

def parse_blocks(text: str) -> list[tuple[int, dict[str, str]]]:
    """Every ```divergence block, as (1-based line of its opening fence, fields).

    A repeated key is kept as the FIRST occurrence and flagged by the caller;
    silently taking the last would let an entry restate `action` after the one
    that was checked.
    """
    blocks = []
    for match in BLOCK.finditer(text):
        line_no = text.count("\n", 0, match.start()) + 1
        fields: dict[str, str] = {}
        duplicates: list[str] = []
        malformed: list[str] = []
        for raw in match.group(1).splitlines():
            if not raw.strip():
                continue
            found = FIELD.match(raw)
            if not found:
                malformed.append(raw.strip())
                continue
            key, value = found.group(1), found.group(2).strip()
            if key in fields:
                duplicates.append(key)
            else:
                fields[key] = value
        fields["__duplicates__"] = ",".join(sorted(set(duplicates)))
        fields["__malformed__"] = "\n".join(malformed)
        blocks.append((line_no, fields))
    return blocks


def unclosed_fences(text: str, blocks: list[tuple[int, dict[str, str]]]) -> list[Problem]:
    """Opening ```divergence fences with no closing fence.

    Without this an unterminated entry is not a block at all: it contributes no
    fields to check and no drift, so a log carrying one passes with `Drift
    count: 0`. A malformed entry has to fail, not disappear.
    """
    opened = [text.count("\n", 0, m.start()) + 1 for m in OPENING_FENCE.finditer(text)]
    closed = {line for line, _ in blocks}
    return [Problem("S0", f"block at line {line}", "```divergence fence is never closed")
            for line in opened if line not in closed]


def load_task(path: Path) -> dict:
    """A task file as JSON, or as the documented flat-YAML subset."""
    text = path.read_text(encoding="utf-8", errors="replace")
    if path.suffix.lower() == ".json":
        return json.loads(text)
    return parse_yaml_subset(text)


def _scalar(raw: str) -> object:
    value = raw.strip()
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        if "[" in inner or "{" in inner:
            raise ValueError(f"nested flow collections are outside the subset: {value}")
        return [_bare(item) for item in inner.split(",")]
    if value.startswith("{"):
        raise ValueError(f"flow mappings are outside the subset: {value}")
    return _bare(value)


def _bare(raw: str) -> str:
    value = raw.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    if value[:1] in ("&", "*", "!", "|", ">"):
        # An anchor, alias, tag or block scalar taken as a plain string is the
        # quiet mis-parse: the file stays readable and means something else.
        raise ValueError(f"value {value!r} is outside the supported subset")
    return value


def parse_yaml_subset(text: str) -> dict:
    """Top-level mapping, block lists, block lists of mappings, inline flow lists.

    Everything else raises. The alternative — parsing approximately and moving
    on — turns an unreadable task file into a silence check that passes.
    """
    root: dict[str, object] = {}
    # (indent of the key that opened this list, indent of its `- ` items, list).
    # The owner indent is what stops an EMPTY nested list from capturing the
    # next sibling: `paths:` with no items under it owns only items indented
    # past it, so a `- id:` back at the outer level pops it instead of joining
    # it. Item indent is -1 until the first item fixes it.
    lists: list[list] = []
    current_item: dict | None = None

    for number, raw in enumerate(text.splitlines(), start=1):
        line = raw.split(" #", 1)[0].rstrip() if " #" in raw else raw.rstrip()
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if line.lstrip().startswith(("---", "...", "&", "*", "<<", "|", ">")):
            raise ValueError(f"line {number}: outside the supported subset: {line.strip()}")
        if "\t" in line[: len(line) - len(line.lstrip())]:
            raise ValueError(f"line {number}: tab indentation is not valid YAML")
        indent = len(line) - len(line.lstrip())
        body = line.strip()

        if body.startswith("- "):
            while lists and (lists[-1][0] >= indent or lists[-1][1] > indent):
                lists.pop()
            if not lists:
                raise ValueError(f"line {number}: list item with no key above it")
            if lists[-1][1] == -1:
                lists[-1][1] = indent
            target = lists[-1][2]
            item = body[2:].strip()
            found = FIELD.match(item)
            if found and found.group(2).strip():
                current_item = {found.group(1): _scalar(found.group(2))}
                target.append(current_item)
            else:
                current_item = None
                target.append(_scalar(item))
            continue

        found = FIELD.match(body)
        if not found:
            raise ValueError(f"line {number}: not a mapping entry: {body}")
        key, value = found.group(1), found.group(2).strip()

        if indent == 0:
            lists.clear()
            current_item = None
            if value:
                root[key] = _scalar(value)
            else:
                opened: list = []
                root[key] = opened
                lists.append([indent, -1, opened])
            continue

        # An indented mapping entry belongs to the list item currently open.
        if current_item is None:
            raise ValueError(f"line {number}: indented key with no list item open: {body}")
        while lists and lists[-1][0] >= indent:
            lists.pop()
        if value:
            current_item[key] = _scalar(value)
        else:
            nested: list = []
            current_item[key] = nested
            lists.append([indent, -1, nested])
    return root


# --------------------------------------------------------------------------- #
# checks
# --------------------------------------------------------------------------- #

def check_schema(line: int, fields: dict[str, str]) -> list[Problem]:
    where = f"block at line {line}"
    problems = []
    if fields.get("__malformed__"):
        for bad in fields["__malformed__"].splitlines():
            problems.append(Problem("S1", where, f"not a field: {bad!r}"))
    if fields.get("__duplicates__"):
        for key in fields["__duplicates__"].split(","):
            problems.append(Problem("S2", where, f"field {key!r} appears more than once"))
    for key in REQUIRED:
        if not fields.get(key, "").strip():
            problems.append(Problem("S3", where, f"missing {key}"))
    grade = fields.get("grade", "")
    if grade and grade not in GRADES:
        problems.append(Problem("S4", where, f"grade {grade!r} is not one of {sorted(GRADES)}"))
    klass = fields.get("class", "")
    if klass and klass not in CLASSES:
        problems.append(Problem("S5", where, f"class {klass!r} is not one of {sorted(CLASSES)}"))
    kind = fields.get("kind", "")
    if kind and kind not in KINDS:
        problems.append(Problem("S11", where, f"kind {kind!r} is not one of {sorted(KINDS)}"))
    if not kind and not klass:
        # One axis is required: `kind` records what happened to the decision,
        # `class` records what it says about the design process.
        problems.append(Problem("S10", where, "neither 'kind' nor 'class' present"))
    if kind == "resolved" and klass:
        # `class` classifies a departure; a close-out is by definition not one.
        # Left legal, the pair is a route around the grade table: `resolved`
        # skips it, so `class: drift` would record a contradiction and take the
        # attesting exemption in the same entry. Mixed axes stay legal for
        # every other kind.
        problems.append(Problem("S12", where,
                                f"kind resolved carries no class, and this one carries {klass!r} — "
                                "a close-out attests compliance, it does not classify a departure"))
    action = fields.get("action", "")
    if action and action not in ACTIONS:
        problems.append(Problem("S6", where, f"action {action!r} is not one of {sorted(ACTIONS)}"))
    at = fields.get("at", "")
    if at and not RFC3339.match(at):
        problems.append(Problem("S7", where, f"at {at!r} is not an RFC 3339 timestamp"))
    attempt = fields.get("attempt", "")
    if attempt and not (attempt.isdigit() and int(attempt) >= 1):
        problems.append(Problem("S8", where, f"attempt {attempt!r} is not a positive integer"))
    if fields.get("decision") == "unlisted" and not fields.get("proposal", "").strip():
        # An unlisted decision is a gap someone has to close; the proposal is
        # the only artifact that carries it back out of the log.
        problems.append(Problem("S9", where, "decision is unlisted, which owes a proposal"))
    return problems


def check_legality(line: int, fields: dict[str, str]) -> list[Problem]:
    grade, action = fields.get("grade", ""), fields.get("action", "")
    if grade not in GRADES or action not in ACTIONS:
        return []  # already reported by the schema check
    where = f"block at line {line}"
    kind = fields.get("kind", "")
    if kind == "resolved":
        # A close-out attests compliance, or records that an open question got
        # settled; it does not contradict, so the grade table does not apply.
        # This is the only legal entry for compliant work in a touched LOCKED
        # area — which the silence check demands an entry for.
        if action not in {"decided", "departed"}:
            return [Problem("L1", where,
                            f"kind resolved licenses 'decided' or 'departed', not {action!r} — "
                            "a close-out attests, it does not halt")]
        return []
    if kind == "blocked":
        if action != "halted":
            return [Problem("L1", where,
                            f"kind blocked licenses 'halted', not {action!r} — "
                            "blocked work that carries on is not blocked")]
        return []
    expected = LEGAL[grade]
    if action != expected:
        return [Problem("L1", where,
                        f"grade {grade} licenses {expected!r}, not {action!r} — "
                        f"{'halting on an assumption costs a round-trip the grading existed to avoid' if expected != 'halted' else 'a locked row is not the executors to flip'}")]
    return []


def check_drift(text: str, blocks: list[tuple[int, dict[str, str]]]) -> list[Problem]:
    """The LAST declared count is the current one.

    The log is append-only, so revising the count means appending a new line
    saying so — never editing the first. Reading the first would force every
    log that later finds drift to choose between failing this check and
    breaking the append-only rule.
    """
    found = list(DRIFT_COUNT.finditer(text))
    declared = found[-1] if found else None
    actual = sum(1 for _, fields in blocks if fields.get("class") == "drift")
    if not declared:
        return [Problem("D1", "log", f"no '**Drift count: N**' line; {actual} entries are classed drift")]
    if int(declared.group(1)) != actual:
        return [Problem("D2", "log",
                        f"declared drift count {declared.group(1)} != {actual} entries classed drift")]
    return []


def resolve(root: Path, relative: str) -> Path | None:
    """A path inside --root, or None. Symlinks are resolved before the check."""
    base = root.resolve()
    try:
        candidate = (base / relative).resolve()
    except OSError:
        return None
    if not candidate.is_relative_to(base):
        return None
    return candidate


def check_evidence(line: int, fields: dict[str, str], root: Path) -> list[Problem]:
    where = f"block at line {line}"
    evidence = fields.get("evidence", "").strip()
    if not evidence:
        return []  # already reported by the schema check

    backticked = BACKTICKED.match(evidence)
    if backticked:
        if not backticked.group("rest").strip(" \t—-:"):
            return [Problem("E1", where,
                            "evidence is a command with no output — the output is the evidence")]
        return []

    # Strip a trailing "— note", which the template encourages.
    citation = evidence.split(" — ")[0].split(" - ")[0].strip()
    found = CITATION.match(citation)
    if not found:
        return [Problem("E2", where,
                        f"evidence {evidence!r} is neither a path:line citation nor a "
                        "backticked command with its output — a sentence is a claim, not evidence")]
    target = resolve(root, found.group("path"))
    if target is None:
        return [Problem("E3", where, f"evidence path {found.group('path')!r} escapes --root")]
    if not target.is_file():
        return [Problem("E4", where, f"evidence path {found.group('path')!r} does not exist")]
    try:
        with target.open("r", encoding="utf-8", errors="replace") as handle:
            total = sum(1 for _ in handle)
    except OSError as exc:
        return [Problem("E5", where, f"evidence path {found.group('path')!r} unreadable: {exc}")]
    start = int(found.group("start"))
    end = int(found.group("end") or start)
    if start < 1 or end < start:
        return [Problem("E6", where, f"evidence range {citation!r} is not a range")]
    if end > total:
        return [Problem("E7", where,
                        f"evidence {citation!r} points past end of file ({total} lines)")]
    return []


def changed_paths(root: Path, base: str) -> list[str]:
    proc = subprocess.run(
        ["git", "-C", str(root), "diff", "--name-only", f"{base}...HEAD"],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"git diff against {base} failed")
    return [line for line in proc.stdout.splitlines() if line.strip()]


def matches(path: str, pattern: str) -> bool:
    """`src/api/**` covers everything under src/api; `*.py` is a plain glob."""
    if pattern.endswith("/**"):
        return path == pattern[:-3] or path.startswith(pattern[:-2])
    return fnmatch(path, pattern)


def check_silence(task: dict, blocks: list[tuple[int, dict[str, str]]],
                  touched: list[str]) -> tuple[list[Problem], list[Skipped]]:
    problems: list[Problem] = []
    skipped: list[Skipped] = []
    logged = {fields.get("decision", "") for _, fields in blocks}
    decisions = task.get("decisions") or []
    if not isinstance(decisions, list):
        raise ValueError("task file: 'decisions' must be a list")
    for entry in decisions:
        if not isinstance(entry, dict):
            raise ValueError(f"task file: decision {entry!r} is not a mapping")
        ident = str(entry.get("id", "")).strip()
        if not ident:
            raise ValueError(f"task file: decision {entry!r} has no id")
        grade = str(entry.get("grade", "")).strip()
        if grade not in GRADES:
            # Not a skip: a typo'd grade silently removes a decision from the
            # check, and the run still reports OK.
            problems.append(Problem("Q2", f"decision {ident}",
                                    f"grade {grade!r} is not one of {sorted(GRADES)}, "
                                    "so this decision was never checked"))
            continue
        if grade != "LOCKED":
            continue
        paths = entry.get("paths") or []
        if isinstance(paths, str):
            paths = [paths]
        if not paths:
            skipped.append(Skipped(ident, "declares no paths, so its area is unknown"))
            continue
        hits = [p for p in touched if any(matches(p, str(g)) for g in paths)]
        if hits and ident not in logged:
            problems.append(Problem(
                "Q1", f"decision {ident}",
                f"LOCKED, and the diff touches {len(hits)} file(s) it governs "
                f"({', '.join(sorted(hits)[:3])}{'…' if len(hits) > 3 else ''}), "
                "with no entry in the log"))
    return problems, skipped


# --------------------------------------------------------------------------- #
# driver
# --------------------------------------------------------------------------- #

def audit(log: Path, root: Path, task: dict | None,
          touched: list[str] | None) -> tuple[list[Problem], list[Skipped]]:
    text = log.read_text(encoding="utf-8", errors="replace")
    blocks = parse_blocks(text)
    problems: list[Problem] = []
    skipped: list[Skipped] = []

    if not blocks:
        # An empty log is legitimate — a task can depart from nothing — but the
        # drift count still has to be there, or a clean run and an unexamined
        # one are the same document.
        skipped.append(Skipped("entries", "the log carries no divergence blocks"))

    problems += unclosed_fences(text, blocks)
    for line, fields in blocks:
        problems += check_schema(line, fields)
        problems += check_legality(line, fields)
        problems += check_evidence(line, fields, root)
    problems += check_drift(text, blocks)

    if task is None:
        skipped.append(Skipped("silence", "no --task, so no decision declares an area"))
    elif touched is None:
        skipped.append(Skipped("silence", "no --base, so no diff to compare against"))
    else:
        found, more = check_silence(task, blocks, touched)
        problems += found
        skipped += more
    return problems, skipped


def render(log: Path, problems: list[Problem], skipped: list[Skipped]) -> str:
    lines = []
    for item in skipped:
        lines.append(str(item))
    for problem in problems:
        lines.append(str(problem))
    verdict = "FAIL" if problems else "OK"
    lines.append(f"{verdict}  {log}: {len(problems)} problem(s), {len(skipped)} skipped")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--log", required=True, type=Path, help="logs/<task-id>.md")
    parser.add_argument("--root", type=Path, default=Path("."),
                        help="repository root that evidence paths are relative to")
    parser.add_argument("--task", type=Path, default=None,
                        help="task file (.json, or the YAML subset) declaring decision areas")
    parser.add_argument("--base", default=None,
                        help="git ref to diff against for the silence check, e.g. origin/main")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    args = parser.parse_args(argv)

    if not args.log.is_file():
        sys.exit(f"error: no such log: {args.log}")
    if not args.root.is_dir():
        sys.exit(f"error: --root {args.root} is not a directory")
    if args.base and not args.task:
        sys.exit("error: --base needs --task; a diff with no declared areas decides nothing")

    task = None
    if args.task:
        if not args.task.is_file():
            sys.exit(f"error: no such task file: {args.task}")
        try:
            task = load_task(args.task)
        except (ValueError, OSError) as exc:
            sys.exit(f"error: {args.task}: {exc}")
        if not isinstance(task, dict):
            sys.exit(f"error: {args.task}: task file must be a mapping")

    touched = None
    if args.base:
        try:
            touched = changed_paths(args.root, args.base)
        except RuntimeError as exc:
            sys.exit(f"error: {exc}")

    try:
        problems, skipped = audit(args.log, args.root, task, touched)
    except (ValueError, OSError) as exc:
        sys.exit(f"error: {exc}")

    if args.json:
        print(json.dumps({
            "log": str(args.log),
            "problems": [{"code": p.code, "where": p.where, "message": p.message} for p in problems],
            "skipped": [{"what": s.what, "why": s.why} for s in skipped],
        }, indent=2))
    else:
        print(render(args.log, problems, skipped))
    return 2 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
