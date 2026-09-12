"""Docs snippets: every Python block in `pages/docs` still compiles against the API.

The sibling of ``docs_floors.py``, which asks whether a symbol is *mentioned* somewhere in
the docs. This asks whether the code around it is *current*, which nothing did: a wrong
snippet renders exactly like a right one, so the failure is silent by construction and a
reader finds it by pasting it.

Three bars, cheapest first, and the last one is the load-bearing one:

1. **Parse.** Every unmarked block is a module that compiles. A block that genuinely
   cannot stand alone is marked ``python fragment`` at its fence — and a marked block that
   parses fine is itself a failure, so the marker cannot be sprinkled to opt out. Both
   halves of that rule are ``skills_check``'s, reused verbatim rather than respelled.
2. **Imports.** Every ``forze*`` symbol a block imports still exists.
3. **Call shapes.** Every call to a symbol the block imported from ``forze*`` binds against
   the live signature — arity and keyword names, never values.

Bar 3 is what this gate exists for. The regression that motivated it —
``build_realtime_mailbox(ctx)`` published on two pages after the function gained a required
keyword-only argument — passes bars 1 and 2: the symbol is still there. Measured over the
corpus, bar 2 finds nothing at all; it stays because it is free once a block is parsed, not
because it earns the gate.

This gate discovers no debt. The corpus passed all three bars the day it was written, so a
green first run is the expected result and not a sign the extractor is broken — which is
why an empty denominator is a failure here, the way it is in ``skills_check``.
"""

from __future__ import annotations

import argparse
import ast
import importlib
import inspect
import sys
from dataclasses import dataclass, field
from pathlib import Path

from tools.skills_check.checks import FRAGMENT_MARKER, is_forze_module
from tools.skills_check.corpus import CodeBlock, parse_document

# ----------------------- #

DOCS_ROOT = Path("pages/docs")

ALLOWED_MARKERS = frozenset({FRAGMENT_MARKER})
"""The markers a docs fence may carry. Same vocabulary as the skills corpus, deliberately."""

SNIPPET_INCLUDE = "--8<--"
"""A block that pulls its body from ``examples/`` is executed by a real test already."""


@dataclass
class Result:
    """One bar's outcome: what it checked, and everything it refused."""

    name: str
    summary: str = ""
    violations: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations


# ....................... #


def load_blocks(root: Path) -> tuple[tuple[CodeBlock, ...], tuple[CodeBlock, ...]]:
    """Every Python block under *root*, split into inline and included.

    Included blocks (``--8<--``) carry no code of their own — the snippet extension pulls
    the body from ``examples/``, which ``tests/unit/test_examples/`` runs. Checking the
    include line would assert nothing and would report a denominator that flatters the
    gate.
    """

    inline: list[CodeBlock] = []
    included: list[CodeBlock] = []

    for path in sorted(root.rglob("*.md")):
        for block in parse_document(path).blocks:
            if not block.is_python:
                continue

            (included if SNIPPET_INCLUDE in block.source else inline).append(block)

    return tuple(inline), tuple(included)


# ....................... #


def check_syntax(blocks: tuple[CodeBlock, ...], unclosed: tuple[CodeBlock, ...]) -> Result:
    """Every unmarked block parses, and every ``fragment`` marker is load-bearing."""

    result = Result(name="syntax")
    marked = 0
    parsed = 0

    # An empty denominator is not a pass: it is what this gate looks like once the
    # extractor stops finding anything — a changed fence convention, a moved docs root —
    # and it is indistinguishable from a corpus with no code in it.
    if not blocks:
        result.violations.append(
            f"{DOCS_ROOT}: not one inline python block found — the extractor found nothing "
            f"to check, which is a checker failure, not a clean corpus"
        )

    for block in unclosed:
        result.violations.append(
            f"{block.doc}:{block.line}: fence is never closed — it swallows the rest of "
            f"the file, and every check below it stops seeing anything"
        )

    for block in blocks:
        where = f"{block.doc}:{block.line}"
        unknown = sorted(set(block.markers) - ALLOWED_MARKERS)

        if unknown:
            result.violations.append(
                f"{where}: unknown fence marker(s) {unknown} — only "
                f"{sorted(ALLOWED_MARKERS)} is defined"
            )

        parses = _parses(block.source) is None

        if FRAGMENT_MARKER in block.markers:
            marked += 1

            if parses:
                result.violations.append(
                    f"{where}: marked `python {FRAGMENT_MARKER}` but parses fine — drop the marker"
                )

            continue

        if parses:
            parsed += 1
        else:
            result.violations.append(f"{where}: does not parse — {_parses(block.source)}")

    unmarked = len(blocks) - marked
    result.summary = f"{parsed}/{unmarked} block(s) parsed, {marked} marked fragment"

    return result


# ....................... #


def check_imports(blocks: tuple[CodeBlock, ...]) -> Result:
    """Every ``forze*`` symbol an unmarked block imports still exists."""

    result = Result(name="imports")
    checked = 0

    for block in _checkable(blocks):
        for module, name in _forze_imports(block.source):
            checked += 1
            where = f"{block.doc}:{block.line}"

            try:
                imported = importlib.import_module(module)

            except Exception as error:
                result.violations.append(
                    f"{where}: `import {module}` fails — {type(error).__name__}: {error}"
                )
                continue

            if name and not hasattr(imported, name):
                result.violations.append(
                    f"{where}: `from {module} import {name}` — {module} has no {name!r}"
                )

    result.summary = f"{checked - len(result.violations)}/{checked} forze import(s) resolve"

    return result


# ....................... #


def check_call_shapes(blocks: tuple[CodeBlock, ...]) -> Result:
    """Every call to an imported ``forze*`` symbol binds against its live signature.

    Arity and keyword names only — the values a snippet passes are prose. A call whose
    arguments are spread (``*args`` / ``**kwargs``) or elided (``...``) states no shape to
    check and is skipped rather than guessed at.
    """

    result = Result(name="call shapes")
    checked = 0

    for block in _checkable(blocks):
        tree = ast.parse(block.source)
        targets = _imported_callables(block.source)

        if not targets:
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
                continue

            target = targets.get(node.func.id)

            if target is None:
                continue

            source = ast.unparse(node)

            if "..." in source:
                continue

            if any(isinstance(arg, ast.Starred) for arg in node.args) or any(
                keyword.arg is None for keyword in node.keywords
            ):
                continue

            try:
                signature = inspect.signature(target)

            except (TypeError, ValueError):
                # A builtin or C-level callable exposes no signature to bind against.
                continue

            checked += 1

            try:
                signature.bind(
                    *([object()] * len(node.args)),
                    **{keyword.arg: object() for keyword in node.keywords if keyword.arg},
                )

            except TypeError as error:
                result.violations.append(
                    f"{block.doc}:{block.line}: `{source.splitlines()[0][:60]}` does not "
                    f"match {node.func.id}{signature} — {error}"
                )

    result.summary = f"{checked - len(result.violations)}/{checked} call(s) match their signature"

    return result


# ....................... #


def _parses(source: str) -> str | None:
    """``None`` when *source* parses, else the reason it does not."""

    try:
        ast.parse(source)

    except SyntaxError as error:
        return str(error)

    return None


# ....................... #


def _checkable(blocks: tuple[CodeBlock, ...]) -> tuple[CodeBlock, ...]:
    """Unmarked blocks that parse — the only ones the later bars can read."""

    return tuple(
        block
        for block in blocks
        if FRAGMENT_MARKER not in block.markers and _parses(block.source) is None
    )


# ....................... #


def _forze_imports(source: str) -> list[tuple[str, str]]:
    """``(module, name)`` for every ``forze*`` import in *source*; name empty for a module."""

    found: list[tuple[str, str]] = []

    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom) and node.module and is_forze_module(node.module):
            found.extend((node.module, alias.name) for alias in node.names)

        elif isinstance(node, ast.Import):
            found.extend((alias.name, "") for alias in node.names if is_forze_module(alias.name))

    return found


# ....................... #


def _imported_callables(source: str) -> dict[str, object]:
    """Local name → live object, for the ``forze*`` names this block imported."""

    found: dict[str, object] = {}

    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.ImportFrom) or not node.module:
            continue

        if not is_forze_module(node.module):
            continue

        try:
            imported = importlib.import_module(node.module)

        except Exception:
            continue

        for alias in node.names:
            target = getattr(imported, alias.name, None)

            if callable(target):
                found[alias.asname or alias.name] = target

    return found


# ....................... #


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DOCS_ROOT)
    args = parser.parse_args(argv)

    inline, included = load_blocks(args.root)
    unclosed = tuple(
        block
        for path in sorted(args.root.rglob("*.md"))
        for block in parse_document(path).blocks
        if not block.closed
    )

    results = [
        check_syntax(inline, unclosed),
        check_imports(inline),
        check_call_shapes(inline),
    ]

    print(
        f"docs snippets: {len(inline)} inline block(s) checked, "
        f"{len(included)} included from examples/ (run by tests/unit/test_examples/)"
    )

    for result in results:
        mark = "ok  " if result.ok else "FAIL"
        print(f"  {mark} {result.name:12} {result.summary}")

        for violation in result.violations:
            print(f"       - {violation}")

    if any(not result.ok for result in results):
        print("\nDocs snippet check failed.")
        return 1

    print("\nDocs snippet check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
