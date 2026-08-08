#!/usr/bin/env python3
"""Supporting tool for the less-code-same-behavior skill: census a symbol's real usage.

Answers "prove dead is dead" and "count before you conclude" with evidence
instead of a single grep. It looks for **every** access pattern the skill names,
because the classic false positive is a `from x import y` grep that misses
attribute access (`module.symbol(...)`) and reports a load-bearing facade as dead.

Patterns counted separately:
  definition   def/class/func/type/const/let/var declarations, and `symbol =`
  from-import  `from x import symbol`, including parenthesized lists
  plain-import `import symbol`
  attribute    `something.symbol` — the pattern a from-import grep misses
  call         `symbol(`
  string       "symbol" / 'symbol' — reflection, config, serialized references
  bare         any other word-boundary mention

Hits are split **internal** (inside the definition's own top-level package
directory) vs **external**, because that split is what decides between
shim-and-move, break-and-migrate, and leave-alone.

  usage_census.py <symbol> [--root DIR] [--internal PREFIX ...] [--json]

Files come from `git ls-files` when the root is a repository (so ignored files
stay ignored), else a filesystem walk. Read-only.

Exit codes: 0 used · 1 usage error · 3 no usage beyond its own definition.
Unknown flags exit 2, from argparse itself.
(a deletion candidate — still confirm the patterns this tool cannot see:
dynamic dispatch, generated code, and other repositories).
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

SKIP_SUFFIXES = {
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".pdf", ".zip", ".gz", ".tar",
    ".whl", ".so", ".dylib", ".dll", ".pyc", ".woff", ".woff2", ".ttf", ".lock",
}
MAX_BYTES = 2_000_000


def tracked_files(root: Path) -> list[Path]:
    """Tracked files when git is available, else a plain filesystem walk.

    git can be absent entirely (containers, minimal CI images); without this
    guard FileNotFoundError aborts the run before the documented fallback.
    """
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), "ls-files"], capture_output=True, text=True, timeout=60
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        proc = None
    if proc is not None and proc.returncode == 0 and proc.stdout.strip():
        return [root / line for line in proc.stdout.splitlines() if line.strip()]
    return [p for p in root.rglob("*") if p.is_file() and ".git" not in p.parts]


def build_patterns(symbol: str) -> dict[str, re.Pattern]:
    s = re.escape(symbol)
    return {
        "definition": re.compile(
            rf"^\s*(?:async\s+)?(?:def|class|func|type|const|let|var|interface|struct|enum)\s+{s}\b"
            rf"|^\s*{s}\s*(?::[^=]+)?=(?!=)"
        ),
        "declaration": re.compile(
            rf"^\s*(?:async\s+)?(?:def|class|func|type|interface|struct|enum|const|let|var)\s+{s}\b"
        ),
        "from-import": re.compile(rf"^\s*from\s+\S+\s+import\s+.*\b{s}\b"),
        # Only meaningful inside a parenthesized import list; on its own a
        # bare line is a standalone reference, not an import.
        "import-list-item": re.compile(rf"^\s*{s}(?:\s+as\s+\w+)?\s*,?\s*$"),
        "plain-import": re.compile(rf"^\s*(?:import|require)\s*\(?\s*[\"']?\S*\b{s}\b"),
        "attribute": re.compile(rf"\.{s}\b"),
        "call": re.compile(rf"\b{s}\s*\("),
        "string": re.compile(rf"[\"']{s}[\"']"),
        "bare": re.compile(rf"\b{s}\b"),
    }


def classify(line: str, patterns: dict[str, re.Pattern], in_import_block: bool) -> str | None:
    """First matching pattern wins; order encodes specificity."""
    for kind in ("definition", "plain-import", "attribute", "call", "string"):
        if patterns[kind].search(line):
            # A parenthesized import list is a from-import, not a bare mention.
            if kind in {"call", "string"} and in_import_block:
                return "from-import"
            return kind
    if patterns["from-import"].search(line):
        return "from-import"
    if in_import_block and patterns["import-list-item"].search(line):
        return "from-import"
    return "bare" if patterns["bare"].search(line) else None


def census(root: Path, symbol: str, internal_prefixes: list[str]) -> dict:
    patterns = build_patterns(symbol)
    word = patterns["bare"]
    hits: list[dict] = []

    for path in tracked_files(root):
        if path.suffix.lower() in SKIP_SUFFIXES or not path.is_file():
            continue
        try:
            if path.stat().st_size > MAX_BYTES:
                continue
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        if symbol not in text:
            continue
        in_import_block = False
        for number, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            if re.match(r"^\s*(?:from|import)\b.*\($", line):
                in_import_block = True
            elif in_import_block and stripped.startswith(")"):
                in_import_block = False
            if not word.search(line):
                continue
            kind = classify(line, patterns, in_import_block)
            if kind:
                entry = {
                    # posix form: the scope tests below split on "/", which a
                    # Windows separator would defeat.
                    "file": path.relative_to(root).as_posix(),
                    "line": number,
                    "kind": kind,
                    "text": stripped[:160],
                }
                hits.append(entry)
                # `symbol = module.symbol` is a facade: recording only the
                # definition would hide the very usage the audit is counting.
                if kind == "definition" and patterns["attribute"].search(line):
                    hits.append({**entry, "kind": "attribute"})

    definition_files = sorted({h["file"] for h in hits if h["kind"] == "definition"})
    # Prefer real declarations when inferring the internal boundary: a plain
    # `symbol = ...` in a test or config would otherwise make that directory
    # "internal" and hide the external usage the audit is counting.
    declaration_files = sorted(
        {h["file"] for h in hits if h["kind"] == "definition" and patterns["declaration"].search(h["text"])}
    )
    inference_source = declaration_files or definition_files
    # A declaration at the scan root has no parent segment. An empty-string
    # prefix would be a prefix of *every* path and silently zero externalUsage,
    # so root membership is tested separately from the directory prefixes.
    prefixes = list(internal_prefixes) or sorted(
        {str(Path(f).parts[0]) + "/" for f in inference_source if Path(f).parts[:-1]}
    )
    root_is_internal = not internal_prefixes and any(
        not Path(f).parts[:-1] for f in inference_source
    )
    for hit in hits:
        at_root = "/" not in hit["file"]
        hit["scope"] = (
            "internal"
            if any(hit["file"].startswith(p) for p in prefixes)
            or (root_is_internal and at_root)
            else "external"
        )

    by_kind: dict[str, int] = {}
    for hit in hits:
        by_kind[hit["kind"]] = by_kind.get(hit["kind"], 0) + 1
    usage = [h for h in hits if h["kind"] != "definition"]
    return {
        "symbol": symbol,
        "root": str(root),
        "internalPrefixes": prefixes + (["<scan root>"] if root_is_internal else []),
        "definitions": definition_files,
        "declarations": declaration_files,
        "counts": {
            "total": len(hits),
            "byKind": by_kind,
            "internalUsage": sum(1 for h in usage if h["scope"] == "internal"),
            "externalUsage": sum(1 for h in usage if h["scope"] == "external"),
            "files": len({h["file"] for h in hits}),
        },
        "hits": hits,
    }


def render(result: dict) -> None:
    counts = result["counts"]
    print(f"symbol: {result['symbol']}   files: {counts['files']}   references: {counts['total']}")
    if result["definitions"]:
        print(f"defined in: {', '.join(result['definitions'])}")
    else:
        print("defined in: (no definition site matched — check the spelling or a generated source)")
    print(f"internal prefixes: {', '.join(result['internalPrefixes']) or '(none inferred)'}")
    print("\nby pattern:")
    for kind in ("definition", "from-import", "plain-import", "attribute", "call", "string", "bare"):
        if kind in counts["byKind"]:
            print(f"  {kind:<13} {counts['byKind'][kind]}")
    print(f"\nusage excluding definitions: {counts['internalUsage']} internal, {counts['externalUsage']} external")

    per_file: dict[str, int] = {}
    for hit in result["hits"]:
        per_file[hit["file"]] = per_file.get(hit["file"], 0) + 1
    print("\ntop files:")
    for name, count in sorted(per_file.items(), key=lambda kv: -kv[1])[:15]:
        print(f"  {count:>4}  {name}")

    if counts["externalUsage"] == 0 and counts["internalUsage"] == 0:
        print("\nNo usage outside the definition. Before deleting, confirm what this cannot see:")
        print("  dynamic dispatch / reflection by computed name, generated code, other repositories.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("symbol")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--internal", action="append", default=[],
        help="path prefix counted as internal (repeatable; default: the definition's top-level dir)",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.symbol):
        sys.exit("error: symbol must be a bare identifier")
    if not args.root.is_dir():
        sys.exit(f"error: {args.root} is not a directory")

    result = census(args.root.resolve(), args.symbol, args.internal)
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        render(result)
    usage = result["counts"]["internalUsage"] + result["counts"]["externalUsage"]
    # A symbol that exists only as its own definition is the deletion
    # candidate this exit code is for.
    return 0 if usage else 3


if __name__ == "__main__":
    sys.exit(main())
