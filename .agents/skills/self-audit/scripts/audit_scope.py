#!/usr/bin/env python3
"""Supporting tool for the self-audit skill: establish scope and measure patch coverage.

  scope           what the audit covers — merge base, commits, diffstat, and the
                  changed files grouped by kind, plus the one-line scope statement
                  the report is supposed to open with
  patch-coverage  which **added** lines a coverage report does not cover — the
                  pass-9 measurement, on the new lines specifically rather than
                  the whole project

Both are read-only git/XML/text reads; neither edits or runs your tests.

Coverage inputs: Cobertura XML (coverage.py `-x`, gocover-cobertura) and LCOV
`.info`. JaCoCo's own XML uses a different element shape and is not read — convert
it with a cobertura reporter first. Paths are matched by longest common suffix, since report paths
are relative to whatever root the runner used.

Exit codes: 0 ok · 1 usage/git error · 2 patch coverage below --min. Unknown flags exit 2, from argparse itself.

Reading the diff is still yours: this tool says where to look and what the tests
missed, never whether the code is right.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

DIFF_HEADER = re.compile(r"^\+\+\+ b/(.*)$")
HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")

TEST_HINTS = ("test", "spec", "conftest", "fixture")
DOC_SUFFIXES = {".md", ".rst", ".txt", ".adoc"}
CONFIG_SUFFIXES = {".yml", ".yaml", ".toml", ".json", ".ini", ".cfg", ".conf", ".lock"}


def git(args: list[str], root: Path) -> str:
    proc = subprocess.run(["git", "-C", str(root), *args], capture_output=True, text=True)
    if proc.returncode != 0:
        sys.exit(f"error: git {' '.join(args[:3])} failed: {proc.stderr.strip()[:300]}")
    return proc.stdout


def detect_base(root: Path) -> str:
    for candidate in ("origin/main", "origin/master", "main", "master"):
        proc = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--verify", "--quiet", candidate],
            capture_output=True, text=True,
        )
        if proc.returncode == 0:
            return candidate
    sys.exit("error: no main/master branch found — pass --base explicitly")


def categorize(path: str) -> str:
    lowered = path.lower()
    suffix = Path(lowered).suffix
    parts = Path(lowered).parts
    if parts and parts[0] in {".github", ".gitlab", ".circleci"}:
        return "ci"
    if any(hint in part for part in parts for hint in TEST_HINTS):
        return "test"
    if suffix in DOC_SUFFIXES or "docs" in parts or "doc" in parts:
        return "docs"
    if suffix in CONFIG_SUFFIXES or Path(lowered).name.startswith("."):
        return "config"
    return "source"


def added_lines(root: Path, base: str, head: str) -> dict[str, list[int]]:
    """Line numbers added per file, from a zero-context diff."""
    diff = git(["diff", "--unified=0", "--no-color", f"{base}...{head}"], root)
    added: dict[str, list[int]] = {}
    current: str | None = None
    for line in diff.splitlines():
        header = DIFF_HEADER.match(line)
        if header:
            current = header.group(1)
            added.setdefault(current, [])
            continue
        hunk = HUNK.match(line)
        if hunk and current:
            start, count = int(hunk.group(1)), int(hunk.group(2) or 1)
            added[current].extend(range(start, start + count))
    return {path: lines for path, lines in added.items() if lines}


def cmd_scope(root: Path, base: str, head: str, as_json: bool) -> int:
    merge_base = git(["merge-base", base, head], root).strip()
    log = git(["log", "--format=%h %s", f"{merge_base}..{head}"], root).strip()
    commits = [line for line in log.splitlines() if line.strip()]
    numstat = git(["diff", "--numstat", f"{merge_base}..{head}"], root).strip()

    files: list[dict] = []
    insertions = deletions = 0
    for line in numstat.splitlines():
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        plus = 0 if parts[0] == "-" else int(parts[0])
        minus = 0 if parts[1] == "-" else int(parts[1])
        insertions += plus
        deletions += minus
        files.append({"path": parts[2], "added": plus, "removed": minus, "kind": categorize(parts[2])})

    by_kind: dict[str, dict[str, int]] = {}
    for entry in files:
        bucket = by_kind.setdefault(entry["kind"], {"files": 0, "added": 0, "removed": 0})
        bucket["files"] += 1
        bucket["added"] += entry["added"]
        bucket["removed"] += entry["removed"]

    statement = (
        f"{len(commits)} commit(s) / {len(files)} file(s) / "
        f"+{insertions}-{deletions} lines, {base}...{head}"
    )
    result = {
        "base": base, "head": head, "mergeBase": merge_base,
        "statement": statement, "commits": commits, "byKind": by_kind, "files": files,
    }
    if as_json:
        print(json.dumps(result, indent=2))
        return 0

    print(f"scope: {statement}")
    print(f"merge base: {merge_base}")
    print(f"\ncommits ({len(commits)}):")
    for commit in commits:
        print(f"  {commit}")
    print("\nby kind:")
    for kind in sorted(by_kind, key=lambda k: -by_kind[k]["added"]):
        bucket = by_kind[kind]
        print(f"  {kind:<7} {bucket['files']:>3} file(s)  +{bucket['added']}-{bucket['removed']}")
    if "source" in by_kind and "test" not in by_kind:
        print("\nnote: source changed with no test files touched — pass 9 starts here.")
    print("\nlargest files:")
    for entry in sorted(files, key=lambda f: -(f["added"] + f["removed"]))[:12]:
        print(f"  +{entry['added']:<5}-{entry['removed']:<5} {entry['kind']:<7} {entry['path']}")
    return 0


def parse_cobertura(path: Path) -> dict[str, dict[int, int]]:
    root = ET.parse(path).getroot()
    sources = [s.text.strip() for s in root.findall(".//sources/source") if s.text]
    coverage: dict[str, dict[int, int]] = {}
    for cls in root.findall(".//class"):
        filename = cls.get("filename")
        if not filename:
            continue
        lines = coverage.setdefault(filename, {})
        for line in cls.findall("./lines/line"):
            number, hits = line.get("number"), line.get("hits")
            if number is not None and hits is not None:
                lines[int(number)] = int(hits)
        for source in sources:
            joined = str(Path(source) / filename)
            coverage.setdefault(joined, {}).update(lines)
    return coverage


def parse_lcov(path: Path) -> dict[str, dict[int, int]]:
    coverage: dict[str, dict[int, int]] = {}
    current: dict[int, int] | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("SF:"):
            current = coverage.setdefault(line[3:].strip(), {})
        elif line.startswith("DA:") and current is not None:
            number, _, hits = line[3:].partition(",")
            try:
                current[int(number)] = int(hits.split(",")[0])
            except ValueError:
                continue
        elif line.startswith("end_of_record"):
            current = None
    return coverage


def match_path(diff_path: str, coverage: dict[str, dict[int, int]]) -> dict[int, int] | None:
    if diff_path in coverage:
        return coverage[diff_path]
    diff_parts = Path(diff_path).parts
    best, best_score, tied = None, 0, False
    for candidate, lines in coverage.items():
        candidate_parts = Path(candidate).parts
        score = 0
        for left, right in zip(reversed(diff_parts), reversed(candidate_parts)):
            if left != right:
                break
            score += 1
        if score > best_score:
            best, best_score, tied = lines, score, False
        elif score == best_score and score > 0:
            tied = True
    # Two report paths can share a longest suffix. Counting one of them would
    # report coverage for a different module, so an ambiguous match is no match.
    return None if tied or not best_score else best


def cmd_patch_coverage(
    root: Path, base: str, head: str, report: Path, minimum: float | None, as_json: bool
) -> int:
    if not report.is_file():
        sys.exit(f"error: {report} not found")
    coverage = parse_lcov(report) if report.suffix == ".info" else parse_cobertura(report)
    if not coverage:
        sys.exit(f"error: no coverage data parsed from {report}")

    added = added_lines(root, base, head)
    per_file, total_measured, total_covered = [], 0, 0
    for path, lines in sorted(added.items()):
        file_coverage = match_path(path, coverage)
        if file_coverage is None:
            per_file.append({"path": path, "measured": 0, "covered": 0, "uncovered": [], "inReport": False})
            continue
        measured = [n for n in lines if n in file_coverage]
        uncovered = [n for n in measured if file_coverage[n] == 0]
        total_measured += len(measured)
        total_covered += len(measured) - len(uncovered)
        per_file.append(
            {
                "path": path, "measured": len(measured),
                "covered": len(measured) - len(uncovered), "uncovered": uncovered, "inReport": True,
            }
        )

    # Never report a percentage when nothing was measured: "100% of zero lines"
    # is a vacuous pass, and a floor check must not clear on it.
    total_added = sum(len(lines) for lines in added.values())
    percent = (total_covered / total_measured * 100) if total_measured else None
    unmeasured_but_changed = total_measured == 0 and total_added > 0
    result = {
        "base": base, "head": head, "report": str(report),
        "addedLines": total_added, "measuredLines": total_measured,
        "coveredLines": total_covered, "patchCoverage": round(percent, 2) if percent is not None else None,
        "files": per_file,
    }
    if as_json:
        print(json.dumps(result, indent=2))
    elif percent is None:
        headline = (
            f"patch coverage: n/a — {total_added} added line(s), none of them in the report"
            if unmeasured_but_changed
            else "patch coverage: n/a — the diff added no lines the report tracks"
        )
        print(headline)
        print(f"diff: {base}...{head}   report: {report}\n")
    else:
        print(f"patch coverage: {percent:.2f}%  ({total_covered}/{total_measured} added lines covered)")
        print(f"diff: {base}...{head}   report: {report}\n")
        for entry in per_file:
            if not entry["inReport"]:
                print(f"  --   {entry['path']} (not in the coverage report)")
            elif entry["uncovered"]:
                shown = ", ".join(str(n) for n in entry["uncovered"][:20])
                more = f" (+{len(entry['uncovered']) - 20} more)" if len(entry["uncovered"]) > 20 else ""
                print(f"  {entry['covered']}/{entry['measured']}  {entry['path']}")
                print(f"        uncovered added lines: {shown}{more}")
            else:
                print(f"  {entry['covered']}/{entry['measured']}  {entry['path']}")
        print(
            "\nRead the uncovered lines before accepting the number: detection branches — "
            "code that only runs when the bug it detects is present — are the ones that must not be dead."
        )
    if unmeasured_but_changed:
        print(
            "\nWARNING: the report tracks none of the added lines — wrong report, stale run, "
            "or a path root the matcher cannot bridge. Do not read this as coverage.",
            file=sys.stderr,
        )
    if minimum is not None:
        if percent is None:
            if unmeasured_but_changed:
                print(f"--min {minimum}% cannot be evaluated: nothing measured", file=sys.stderr)
                return 2
        elif percent < minimum:
            print(f"\nbelow --min {minimum}%", file=sys.stderr)
            return 2
    return 0


def main() -> int:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--root", type=Path, default=Path.cwd())
    common.add_argument("--base", help="base ref (default: origin/main, main, or master)")
    common.add_argument("--head", default="HEAD")
    common.add_argument("--json", action="store_true")

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("scope", parents=[common])
    coverage_parser = sub.add_parser("patch-coverage", parents=[common])
    coverage_parser.add_argument("--report", type=Path, required=True, help="coverage.xml or lcov .info")
    coverage_parser.add_argument("--min", dest="minimum", type=float, help="fail below this percentage")

    args = parser.parse_args()
    root = args.root.resolve()
    if not (root / ".git").exists() and not (root / ".git").is_file():
        proc = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--git-dir"], capture_output=True, text=True
        )
        if proc.returncode != 0:
            sys.exit(f"error: {root} is not a git repository")
    minimum = getattr(args, "minimum", None)
    if minimum is not None and not (0.0 <= minimum <= 100.0):
        # nan compares false against everything, so it would clear any gate.
        sys.exit(f"error: --min must be a percentage between 0 and 100 (got {minimum})")
    base = args.base or detect_base(root)

    if args.cmd == "scope":
        return cmd_scope(root, base, args.head, args.json)
    return cmd_patch_coverage(root, base, args.head, args.report, args.minimum, args.json)


if __name__ == "__main__":
    sys.exit(main())
