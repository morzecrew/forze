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
are relative to whatever root the runner used. An XML report that declares
entities is refused rather than parsed (see reject_entity_declarations).

Exit codes: 0 ok · 1 usage/git error · 2 patch coverage below --min. Unknown flags exit 2, from argparse itself.

Reading the diff is still yours: this tool says where to look and what the tests
missed, never whether the code is right.
"""

from __future__ import annotations

import argparse
import codecs
import json
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
import xml.parsers.expat as expat
from pathlib import Path

# A deleted file's +++ line is /dev/null. Matching only `b/<path>` left the
# parser pointing at the previous file, so the deletion's hunks — and every
# hunk after it until the next header — were attributed to the wrong path.
XML_ENCODING = re.compile(r"\s+encoding\s*=\s*(['\"])[^'\"]*\1")

DIFF_HEADER = re.compile(r"^\+\+\+ (b/.*|/dev/null)$")
HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")

class EntityDeclared(Exception):
    """An entity declaration was found in the prolog."""


class PrologEnded(Exception):
    """The root element was reached with no declaration seen."""

TEST_HINTS = ("test", "spec", "conftest", "fixture")
DOC_SUFFIXES = {".md", ".rst", ".txt", ".adoc"}
CONFIG_SUFFIXES = {".yml", ".yaml", ".toml", ".json", ".ini", ".cfg", ".conf", ".lock"}


def git(args: list[str], root: Path) -> str:
    # core.quotePath=false: git otherwise C-quotes any non-ASCII path
    # ("caf\303\251.py") in diff headers and numstat output, and the quoted name
    # matches nothing downstream — the file's added lines go uncounted.
    proc = subprocess.run(
        ["git", "-C", str(root), "-c", "core.quotePath=false", *args],
        capture_output=True, text=True,
    )
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
            target = header.group(1)
            # A deletion contributes no added lines; drop the pointer rather
            # than leaving it on the file before it.
            current = None if target == "/dev/null" else target[2:]
            if current is not None:
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


def is_lcov_report(path: Path, head: bytes) -> bool:
    """Recognise LCOV by its content, with the extension only as a tiebreak.

    Dispatching on `.info` alone sent coverage.lcov and lcov.dat to the XML
    parser, which failed with a ParseError traceback rather than a message. The
    BOM is stripped first: a byte-order mark is whitespace to no one, so a
    BOM-prefixed Cobertura report named .lcov would otherwise be read as LCOV
    and come back empty.
    """
    head = head[:4096]
    for bom in (codecs.BOM_UTF8, codecs.BOM_UTF32_LE, codecs.BOM_UTF32_BE,
                codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE):
        if head.startswith(bom):
            head = head[len(bom):]
            break
    # Drop the NUL padding a UTF-16 or UTF-32 encoding leaves between ASCII
    # bytes: without this the '<' is never at the front and a wide-encoded
    # Cobertura report named .lcov was dispatched to the LCOV parser, which
    # found no coverage at all.
    compact = head.replace(b"\x00", b"").lstrip()
    if compact.startswith(b"<"):
        return False
    if b"SF:" in compact or b"TN:" in compact:
        return True
    return path.suffix.lower() in {".info", ".lcov", ".dat"}


def read_head(path: Path, size: int = 4096) -> bytes:
    """The first `size` bytes, so dispatch does not load the whole report."""
    try:
        with path.open("rb") as handle:
            return handle.read(size)
    except OSError as exc:
        sys.exit(f"error: cannot read {path}: {exc}")


def reject_entity_declarations(data: bytes, path: Path) -> None:
    """Refuse a report that declares an XML entity.

    Refusing every DTD would refuse real reports — coverage.py emits a DOCTYPE
    naming an external DTD, which ElementTree never retrieves. The hazard is an
    entity *declaration*, which ElementTree does expand: nesting multiplies the
    text tenfold per level, so a few hundred bytes of declarations become
    however much memory the author cares to ask for. No coverage writer emits
    one.

    Expat decides what a declaration is, rather than a regex hunting for the
    prolog: any comment holding a `<` ended that scan early, and a DOCTYPE
    after it was never examined at all. Parsing stops at the root element, so
    the cost is the prolog rather than the document.
    """
    parser = expat.ParserCreate()

    def refuse(*_args):
        raise EntityDeclared()

    def stop(*_args):
        raise PrologEnded()

    parser.EntityDeclHandler = refuse
    parser.UnparsedEntityDeclHandler = refuse
    parser.StartElementHandler = stop
    try:
        parser.Parse(data, True)
    except PrologEnded:
        return
    except EntityDeclared:
        sys.exit(
            f"error: {path} declares XML entities. Coverage writers do not emit those, "
            "and expanding them exhausts memory — refusing to parse. Regenerate the "
            "report from your test runner."
        )
    except (expat.ExpatError, ValueError):
        # Malformed, or an encoding expat refuses outright — it raises
        # ValueError, not ExpatError, for a multi-byte encoding declaration.
        # Let the real parse below report it, so the message a user sees for a
        # report that cannot be read comes from one place.
        return


def wide_encoding(data: bytes) -> str | None:
    """The UTF-16/32 form of these bytes, by BOM or by the XML spec's own rule.

    Appendix F of the XML spec detects an encoding from the first four bytes,
    because a conforming document begins with `<`. That covers the BOM-less
    case, which matters: Python's "utf-32-be" codec writes no BOM at all.
    """
    for bom, encoding in (
        (codecs.BOM_UTF32_LE, "utf-32-le"), (codecs.BOM_UTF32_BE, "utf-32-be"),
        (codecs.BOM_UTF16_LE, "utf-16-le"), (codecs.BOM_UTF16_BE, "utf-16-be"),
    ):
        if data.startswith(bom):
            return encoding
    for opener, encoding in (
        (b"<\x00\x00\x00", "utf-32-le"), (b"\x00\x00\x00<", "utf-32-be"),
        (b"<\x00", "utf-16-le"), (b"\x00<", "utf-16-be"),
    ):
        if data.startswith(opener):
            return encoding
    return None


def to_parseable_xml(data: bytes) -> bytes:
    """Re-encode to UTF-8 when the bytes are in a form expat cannot read.

    Expat rejects UTF-32 outright and raises on a multi-byte encoding
    declaration, so recognising a wide-encoded report and routing it to the XML
    parser — as the dispatch now does — would only trade "no coverage found"
    for a parse error. Converting is what recognising it has to mean.
    """
    encoding = wide_encoding(data)
    if encoding is None:
        return data
    text = data.decode(encoding, "replace").lstrip("﻿")
    # Drop the declared encoding: it describes neither these bytes nor what
    # expat is about to read.
    return XML_ENCODING.sub("", text, count=1).encode("utf-8")


def parse_cobertura(path: Path) -> dict[str, dict[int, int]]:
    data = to_parseable_xml(path.read_bytes())
    reject_entity_declarations(data, path)
    try:
        # Entity declarations are refused above; bare nosec because bandit reads
        # anything trailing it as further test ids.
        root = ET.fromstring(data)  # nosec B314
    except (ET.ParseError, ValueError) as exc:
        sys.exit(
            f"error: {path} is not parseable XML ({exc}). Expected a Cobertura report "
            "or an LCOV file — check that the report is the one your runner wrote."
        )
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
    if tied or not best_score:
        return None
    # A bare filename match is not identification: src/app.py and other/app.py
    # share app.py and nothing else, and attributing one's coverage to the other
    # is a wrong answer wearing a number. Demand either a directory component
    # too, or that the match account for the whole diff path (a root-level file
    # legitimately matches on its name alone).
    if best_score < 2 and best_score != len(diff_parts):
        return None
    return best


def cmd_patch_coverage(
    root: Path, base: str, head: str, report: Path, minimum: float | None, as_json: bool
) -> int:
    if not report.is_file():
        sys.exit(f"error: {report} not found")
    coverage = (
        parse_lcov(report)
        if is_lcov_report(report, read_head(report))
        else parse_cobertura(report)
    )
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
        # Written as a range test rather than `< 0 or > 100` so that nan, which
        # compares false against everything, fails it too instead of sailing
        # through to clear whatever gate --min was meant to enforce.
        sys.exit(f"error: --min must be a percentage between 0 and 100 (got {minimum})")
    base = args.base or detect_base(root)

    if args.cmd == "scope":
        return cmd_scope(root, base, args.head, args.json)
    return cmd_patch_coverage(root, base, args.head, args.report, args.minimum, args.json)


if __name__ == "__main__":
    sys.exit(main())
