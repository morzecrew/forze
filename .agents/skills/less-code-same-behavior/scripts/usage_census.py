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
  comment      named only in a comment — recorded as evidence, never as usage

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
# A complete character literal: 'x', '\n', '\x41', '\u{1F600}'. Anything else
# beginning with an apostrophe is a lifetime, a label, or a prime in a name.
CHAR_LITERAL = re.compile(r"'(?:\\(?:u\{[0-9A-Fa-f]+\}|x[0-9A-Fa-f]{2}|.)|[^\\'])'")
# Languages where an apostrophe opens a character literal rather than a string.
# Everywhere else it quotes an ordinary string, and the two cannot share a rule:
# one reading loses code after a lifetime, the other loses code after a string.
APOSTROPHE_IS_CHAR = {
    ".rs", ".c", ".h", ".cc", ".cpp", ".hpp", ".go", ".java",
    ".cs", ".swift", ".kt", ".scala",
}


def tracked_files(root: Path) -> list[Path]:
    """Tracked files when git is available, else a plain filesystem walk.

    git can be absent entirely (containers, minimal CI images); without this
    guard FileNotFoundError aborts the run before the documented fallback.
    """
    try:
        # -z: without it git C-quotes any path holding a non-ASCII or unusual
        # byte ("caf\303\251.py"). Those names never resolve, so the files were
        # skipped in silence — and a symbol whose only reference lived in one
        # would be reported unused, which is the verdict that ends in a delete.
        # Bytes, not text: a tracked name the locale cannot decode would
        # otherwise raise and abort the whole census. surrogateescape keeps
        # arbitrary path bytes round-trippable through the filesystem calls.
        proc = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z"],
            capture_output=True, timeout=60,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        proc = None
    if proc is not None and proc.returncode == 0:
        names = proc.stdout.decode(sys.getfilesystemencoding(), "surrogateescape")
        tracked = [root / name for name in names.split("\0") if name]
        if tracked:
            return tracked
        # A repository that tracks nothing is still a repository, so an empty
        # listing is not "no git here". Falling through to the walk would scan
        # exactly the ignored files tracked mode promised to skip, and scanning
        # nothing would report every symbol as a deletion candidate. Neither is
        # an answer worth giving, so refuse to answer at all.
        sys.exit(
            f"error: {root} is a git repository with no tracked files — nothing to census. "
            "`git add` the sources first, or point --root at a non-repository directory."
        )
    return [p for p in root.rglob("*") if p.is_file() and ".git" not in p.parts]


def within_prefix(path: str, prefix: str) -> bool:
    """Prefix test on path components, not on raw characters.

    A raw string prefix makes `--internal pkg` swallow `pkg2/file.py`: those
    hits score internal, external usage is undercounted, and the internal vs
    external split is the whole basis of the shim-or-delete decision.
    """
    clean = prefix.replace("\\", "/").rstrip("/")
    while clean.startswith("./"):
        clean = clean[2:]
    # `.` and `./` name the scan root, so everything is inside them. Reducing
    # them to the empty string instead marked every hit external and inverted
    # the split this function exists to get right.
    if clean in {"", "."}:
        return True
    return path == clean or path.startswith(clean + "/")


# Modifiers that can precede a declaration keyword in the languages covered.
MODIFIERS = (
    r"(?:(?:export|default|public|private|protected|internal|static|final|async"
    r"|pub(?:\([^)]*\))?)\s+)*"  # pub(crate), pub(super), … are Rust visibility
)
# `function` for JS/PHP, `fn` for Rust — without them a real declaration scored
# as a call, so an unused function could never reach the exit-3 deletion
# candidate the tool exists to identify.
DECL_KEYWORDS = r"def|class|func|function|fn|type|const|let|var|interface|struct|enum"


def build_patterns(symbol: str) -> dict[str, re.Pattern]:
    s = re.escape(symbol)
    # Go methods carry a receiver between the keyword and the name
    # (`func (r *Runtime) Helper() error`), which the plain keyword form misses.
    receiver = rf"^\s*func\s+\([^)]*\)\s*{s}\b"
    return {
        "definition": re.compile(
            rf"^\s*{MODIFIERS}(?:{DECL_KEYWORDS})(?:\s*\*\s*|\s+){s}\b"
            rf"|{receiver}"
            rf"|^\s*{s}\s*(?::[^=]+)?=(?!=)"
        ),
        "declaration": re.compile(
            rf"^\s*{MODIFIERS}(?:{DECL_KEYWORDS})(?:\s*\*\s*|\s+){s}\b"
            rf"|{receiver}"
        ),
        "from-import": re.compile(rf"^\s*from\s+\S+\s+import\s+.*\b{s}\b"),
        # Only meaningful inside a parenthesized import list; on its own a
        # bare line is a standalone reference, not an import. Neighbours on the
        # same line are allowed — `helper, other,` is still an import of both.
        # `original as helper` binds the symbol too, so the symbol may appear
        # on either side of an `as`.
        "import-list-item": re.compile(
            rf"^[\s(]*(?:\w+(?:\s+as\s+\w+)?\s*,\s*)*"
            rf"(?:{s}(?:\s+as\s+\w+)?|\w+\s+as\s+{s})"
            rf"(?:\s*,\s*\w+(?:\s+as\s+\w+)?)*\s*,?\s*\)?\s*$"
        ),
        "plain-import": re.compile(rf"^\s*(?:import|require)\s*\(?\s*[\"']?\S*\b{s}\b"),
        "attribute": re.compile(rf"\.{s}\b"),
        "call": re.compile(rf"\b{s}\s*\("),
        "string": re.compile(rf"[\"']{s}[\"']"),
        "bare": re.compile(rf"\b{s}\b"),
    }


# Per-language, because the marker is not universal: `//` is floor division in
# Python and `#` is not a comment in C or JavaScript, so a blanket rule would
# drop real references. Unlisted suffixes strip nothing.
LINE_COMMENTS = {
    ".py": ("#",), ".pyi": ("#",), ".sh": ("#",), ".bash": ("#",), ".zsh": ("#",),
    ".rb": ("#",), ".yml": ("#",), ".yaml": ("#",), ".toml": ("#",), ".cfg": ("#",),
    ".pl": ("#",), ".r": ("#",), ".tf": ("#",),
    ".js": ("//",), ".mjs": ("//",), ".cjs": ("//",), ".jsx": ("//",),
    ".ts": ("//",), ".tsx": ("//",), ".go": ("//",), ".rs": ("//",),
    ".java": ("//",), ".kt": ("//",), ".swift": ("//",), ".scala": ("//",),
    ".c": ("//",), ".h": ("//",), ".cc": ("//",), ".cpp": ("//",), ".hpp": ("//",),
    ".cs": ("//",), ".php": ("//", "#"), ".sql": ("--",), ".lua": ("--",),
}


# Languages whose block comments run across lines. Tracked with a carry flag,
# because `/* helper */` reaching classify() counted as a live reference and a
# dead symbol mentioned in one could dodge the deletion verdict.
BLOCK_COMMENTS = {
    ".js": ("/*", "*/"), ".mjs": ("/*", "*/"), ".cjs": ("/*", "*/"),
    ".jsx": ("/*", "*/"), ".ts": ("/*", "*/"), ".tsx": ("/*", "*/"),
    ".go": ("/*", "*/"), ".rs": ("/*", "*/"), ".java": ("/*", "*/"),
    ".kt": ("/*", "*/"), ".swift": ("/*", "*/"), ".scala": ("/*", "*/"),
    ".c": ("/*", "*/"), ".h": ("/*", "*/"), ".cc": ("/*", "*/"),
    ".cpp": ("/*", "*/"), ".hpp": ("/*", "*/"), ".cs": ("/*", "*/"),
    ".php": ("/*", "*/"), ".css": ("/*", "*/"), ".scss": ("/*", "*/"),
}


def strip_comments(
    line: str, markers: tuple[str, ...], block: tuple[str, str] | None, inside: bool,
    apostrophe_is_char: bool = False,
) -> tuple[str, bool]:
    """Strip line and block comments in one left-to-right pass.

    One scanner, because the two forms interact: handling blocks first let a
    `/*` sitting inside a `// …` comment open a block that swallowed every
    following line, and a live reference below it then went uncounted — a used
    symbol made to look deletable. A line comment ends the line; a block
    comment opened outside one carries to the next.

    Quotes are tracked so a marker inside a string literal does not truncate
    real code.
    """
    opener, closer = block if block else (None, None)
    out: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(line):
        if inside:
            end = line.find(closer, index) if closer else -1
            if end == -1:
                return "".join(out), True
            index = end + len(closer)
            inside = False
            continue
        char = line[index]
        if quote is not None:
            if char == "\\":
                out.append(line[index:index + 2])
                index += 2
                continue
            if char == quote:
                quote = None
            out.append(char)
            index += 1
            continue
        if char == "'" and apostrophe_is_char:
            # Only where the language says so. In Rust, C and Go an apostrophe
            # opens a character literal — or, in `&'a str` and `'outer:`, no
            # literal at all. Treating those as strings ran the "inside a
            # string" state to the end of the line and left a block comment
            # unstripped. Elsewhere — Python, JavaScript, PHP, shell — `'`
            # quotes an ordinary string, and refusing to open one there let a
            # `#` *inside* a string truncate the line and drop live code.
            literal = CHAR_LITERAL.match(line, index)
            if literal:
                out.append(literal.group(0))
                index = literal.end()
            else:
                out.append(char)
                index += 1
            continue
        if char in "\"`" or char == "'":
            quote = char
            out.append(char)
            index += 1
            continue
        if any(line.startswith(marker, index) for marker in markers):
            return "".join(out), False
        if opener and line.startswith(opener, index):
            index += len(opener)
            inside = True
            continue
        out.append(char)
        index += 1
    return "".join(out), inside


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
        suffix = path.suffix.lower()
        markers = LINE_COMMENTS.get(suffix, ())
        block = BLOCK_COMMENTS.get(suffix)
        in_block_comment = False
        in_import_block = False
        for number, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            # Comment stripping comes first: a line inside a block comment must
            # not drive the import-block state either.
            code, in_block_comment = strip_comments(
                line, markers, block, in_block_comment, suffix in APOSTROPHE_IS_CHAR
            )
            if re.match(r"^\s*(?:from|import)\b.*\($", code):
                in_import_block = True
            elif in_import_block and code.strip().startswith(")"):
                in_import_block = False
            if not word.search(line):
                continue
            # Named only in a comment: kept as evidence, excluded from usage.
            kind = classify(code, patterns, in_import_block) if word.search(code) else "comment"
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
                # First match wins, so a definition line stops there and its
                # right-hand side is lost — and `symbol = module.symbol` is
                # exactly how a live symbol hides behind a facade. Classify the
                # RHS separately and record that too.
                if kind == "definition" and "=" in code:
                    rhs = code.split("=", 1)[1]
                    rhs_kind = classify(rhs, patterns, False) if word.search(rhs) else None
                    if rhs_kind and rhs_kind != "definition":
                        hits.append({**entry, "kind": rhs_kind})

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
            if any(within_prefix(hit["file"], p) for p in prefixes)
            or (root_is_internal and at_root)
            else "external"
        )

    by_kind: dict[str, int] = {}
    for hit in hits:
        by_kind[hit["kind"]] = by_kind.get(hit["kind"], 0) + 1
    # A mention in a comment is evidence, not a use: it must not keep a dead
    # symbol looking alive, and it must not count toward the deletion verdict.
    usage = [h for h in hits if h["kind"] not in {"definition", "comment"}]
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
    for kind in ("definition", "from-import", "plain-import", "attribute", "call", "string", "bare", "comment"):
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
