#!/usr/bin/env python3
"""Docs floors: a public contract symbol may not ship without a doc that mentions it.

The sibling of ``coverage_floors.py``, aimed at the debt that recurred through six
framework audits: a plane lands, its ports and specs work, and the docs catch up a
release or three later — or never, because nothing fails when they do not. Coverage had
exactly this shape until a per-package floor made a thin package a build failure rather
than a note in a review. This checker does the same for documentation.

Three properties, all of them cheap to check and none of them a judgement about prose:

1. **Symbol coverage.** Every public ``DepKey`` and every ``*Spec`` class declared under
   ``forze.application.contracts`` is mentioned somewhere in ``pages/docs``, by its symbol
   name or (for a dep key) by its wire name. Mention is a deliberately low bar: this gate
   asserts a symbol is *reachable* from the docs, not that it is well explained. A bar
   that tried to judge quality would either be gamed or ignored.

2. **Nav integrity, both directions.** Every nav entry resolves to a file, and every doc
   file appears in the nav — an orphan page is invisible to readers and rots unnoticed.
   Snippet includes are legitimately not nav entries and are declared as globs.

3. **Link integrity.** Every relative markdown link between docs resolves. A link into a
   build-output tree (the rendered diagrams, which are gitignored) is resolved against the
   source that generates it, so the check holds on a checkout that has never run the build.

Symbols are collected **by import**, like the conformance manifest and the mock-coverage
guard, for the same reason: a key re-exported under another name is the same object, which
a grep would double-count and a regex over a multi-line declaration would miss.

Policy lives in ``pyproject.toml``:

    [tool.docs_floors]
    docs_root = "pages/docs"
    nav_config = "pages/zensical.toml"
    orphan_allow = ["dst/_generated/*.md"]

    [tool.docs_floors.generated_links]
    prefix = "_diagrams"
    source_dir = "pages/diagrams"
    source_suffix = ".d2"

    [[tool.docs_floors.exempt_groups]]
    kind = "..."
    symbols = [...]
    reason = \"\"\"...\"\"\"

Exemptions are grouped and reasoned rather than listed flat, matching the conformance
manifest: "not documented" becomes data a reviewer sees instead of an absence they have to
notice. Every exempt symbol must still exist, so the table cannot rot, and a symbol that
gains a doc page must leave the table — the gate fails on a redundant exemption, which is
what makes the standing debt shrink instead of ossify.

Usage (from the repo root):

    python .github/scripts/docs_floors.py
"""

from __future__ import annotations

import argparse
import importlib
import pkgutil
import re
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ----------------------- #

_CONFIG_TABLE = "docs_floors"
_CONTRACTS_PACKAGE = "forze.application.contracts"
_LINK_PATTERN = re.compile(r"\]\(([^)]+)\)")
_EXTERNAL_PREFIXES = ("http://", "https://", "mailto:", "//", "/")


@dataclass(frozen=True)
class Symbol:
    """One public contract symbol the docs are expected to mention."""

    name: str
    """The exported symbol name — what a doc page would actually write."""

    alias: str
    """A second accepted spelling; a dep key's wire name, else the symbol name again."""

    kind: str
    """``dep_key`` or ``spec`` — reported so a failure says what kind of thing is missing."""

    module: str
    """Declaring module, for the failure message."""

    @property
    def plane(self) -> str:
        """The contracts subpackage this belongs to, used to group the report."""

        tail = self.module.removeprefix(f"{_CONTRACTS_PACKAGE}.")

        return tail.split(".")[0] if tail != self.module else "contracts"


@dataclass(frozen=True)
class ExemptGroup:
    """A declared set of symbols with no doc yet, and why."""

    kind: str
    symbols: frozenset[str]
    reason: str


@dataclass(frozen=True)
class GeneratedLinks:
    """A docs subtree the build writes into, checked against the sources that write it.

    Diagrams are rendered from ``.d2`` at build time and the output directory is
    gitignored, so a checkout has the sources and not the SVGs. Skipping the subtree
    outright would be the easy fix and the wrong one: the link check exists to catch a
    misspelled or deleted target, and a blanket skip retires it for exactly the links most
    likely to rot (a renamed diagram breaks silently and only shows as a hole in the built
    page). Resolving the output back to its source keeps the check honest on a machine
    that has never run the build.
    """

    prefix: Path
    """Docs-root-relative directory the build writes into."""

    source_dir: Path
    """Repo-relative directory holding the sources."""

    source_suffix: str
    """Extension of a source file, replacing the link target's own."""

    def source_for(self, target: Path, docs_root: Path) -> Path | None:
        """The source that would generate *target*, or ``None`` if it is not an output."""

        try:
            relative = target.relative_to(docs_root.resolve())
        except ValueError:
            return None

        if not relative.is_relative_to(self.prefix):
            return None

        return self.source_dir / f"{target.stem}{self.source_suffix}"


@dataclass(frozen=True)
class Policy:
    """Where the docs live and what is allowed to be missing."""

    docs_root: Path
    nav_config: Path
    orphan_allow: tuple[str, ...] = ()
    exempt_groups: tuple[ExemptGroup, ...] = ()
    generated: GeneratedLinks | None = None
    _exempt: dict[str, ExemptGroup] = field(default_factory=dict, compare=False)

    def exempt_for(self, symbol: str) -> ExemptGroup | None:
        return self._exempt.get(symbol)


# ----------------------- #


def load_policy(pyproject_path: Path) -> Policy:
    """Read the docs policy from ``[tool.docs_floors]``."""

    with pyproject_path.open("rb") as fh:
        config = tomllib.load(fh)

    try:
        table = config["tool"][_CONFIG_TABLE]
    except KeyError:
        raise SystemExit(
            f"error: [tool.{_CONFIG_TABLE}] table missing from {pyproject_path}"
        ) from None

    groups = tuple(
        ExemptGroup(
            kind=str(entry["kind"]),
            symbols=frozenset(str(name) for name in entry.get("symbols", ())),
            reason=str(entry.get("reason", "")).strip(),
        )
        for entry in table.get("exempt_groups", ())
    )
    index: dict[str, ExemptGroup] = {}

    for group in groups:
        for name in group.symbols:
            index[name] = group

    raw_generated = table.get("generated_links")
    generated = (
        GeneratedLinks(
            prefix=Path(str(raw_generated["prefix"])),
            source_dir=Path(str(raw_generated["source_dir"])),
            source_suffix=str(raw_generated["source_suffix"]),
        )
        if raw_generated is not None
        else None
    )

    return Policy(
        docs_root=Path(str(table["docs_root"])),
        nav_config=Path(str(table["nav_config"])),
        orphan_allow=tuple(str(pattern) for pattern in table.get("orphan_allow", ())),
        exempt_groups=groups,
        generated=generated,
        _exempt=index,
    )


def discover_symbols() -> dict[str, Symbol]:
    """Every public ``DepKey`` and ``*Spec`` declared under ``contracts/``, by import."""

    from forze.application.contracts.deps import DepKey

    contracts = importlib.import_module(_CONTRACTS_PACKAGE)
    found: dict[str, Symbol] = {}

    for module_info in pkgutil.walk_packages(contracts.__path__, f"{_CONTRACTS_PACKAGE}."):
        module = importlib.import_module(module_info.name)

        for attribute in dir(module):
            if attribute.startswith("_"):
                continue

            value = getattr(module, attribute, None)

            if isinstance(value, DepKey):
                found.setdefault(
                    attribute,
                    Symbol(
                        name=attribute,
                        alias=value.name,
                        kind="dep_key",
                        module=module_info.name,
                    ),
                )

            elif (
                isinstance(value, type)
                and attribute.endswith("Spec")
                and getattr(value, "__module__", "").startswith("forze.")
            ):
                found.setdefault(
                    attribute,
                    Symbol(
                        name=attribute,
                        alias=attribute,
                        kind="spec",
                        module=module_info.name,
                    ),
                )

    return found


def nav_entries(nav_config: Path) -> list[str]:
    """Flatten the nav tree into the doc paths it names."""

    config = tomllib.loads(nav_config.read_text(encoding="utf-8"))
    nav = config.get("project", {}).get("nav", config.get("nav"))
    out: list[str] = []

    def _walk(node: Any) -> None:
        if isinstance(node, str):
            out.append(node)
        elif isinstance(node, list):
            for child in node:
                _walk(child)
        elif isinstance(node, dict):
            for child in node.values():
                _walk(child)

    _walk(nav)

    return out


# ----------------------- #


def check_symbols(
    symbols: dict[str, Symbol],
    corpus: str,
    policy: Policy,
) -> tuple[list[str], set[str]]:
    """Undocumented-symbol and stale-exemption violations, plus the documented set."""

    violations: list[str] = []
    documented: set[str] = set()

    for name, symbol in sorted(symbols.items()):
        is_documented = name in corpus or symbol.alias in corpus
        exempt = policy.exempt_for(name)

        if is_documented:
            documented.add(name)

            if exempt is not None:
                violations.append(
                    f"{name}: documented, but still listed in an exempt group "
                    f"({exempt.kind}) — delete the entry so the table keeps shrinking"
                )

            continue

        if exempt is None:
            violations.append(
                f"{name} ({symbol.kind}, {symbol.module}): no page under "
                f"{policy.docs_root} mentions it — document it, or add it to an "
                f"[[tool.{_CONFIG_TABLE}.exempt_groups]] entry with a reason"
            )

    known = set(symbols)

    for group in policy.exempt_groups:
        for name in sorted(group.symbols - known):
            violations.append(
                f"{name}: stale exemption ({group.kind}) — no such contract symbol; "
                f"delete it from [[tool.{_CONFIG_TABLE}.exempt_groups]]"
            )

    return violations, documented


def check_nav(policy: Policy) -> list[str]:
    """Nav entries resolve to files, and every file is reachable from the nav."""

    violations: list[str] = []
    entries = nav_entries(policy.nav_config)
    on_disk = {
        path.relative_to(policy.docs_root).as_posix()
        for path in policy.docs_root.rglob("*.md")
    }

    for entry in sorted(set(entries) - on_disk):
        violations.append(f"{entry}: nav entry has no file under {policy.docs_root}")

    for orphan in sorted(on_disk - set(entries)):
        if any(Path(orphan).match(pattern) for pattern in policy.orphan_allow):
            continue

        violations.append(
            f"{orphan}: page is not reachable from the nav — add it to "
            f"{policy.nav_config}, or declare its glob in orphan_allow"
        )

    return violations


def check_links(policy: Policy) -> list[str]:
    """Every relative markdown link between docs resolves to a real file."""

    violations: list[str] = []

    for page in sorted(policy.docs_root.rglob("*.md")):
        for match in _LINK_PATTERN.finditer(page.read_text(encoding="utf-8")):
            target = match.group(1).split("#")[0].strip()

            if not target or target.startswith(_EXTERNAL_PREFIXES):
                continue

            resolved = (page.parent / target).resolve()

            if resolved.exists():
                continue

            source = (
                policy.generated.source_for(resolved, policy.docs_root)
                if policy.generated is not None
                else None
            )

            if source is not None:
                # A build output: absent is expected, so the source is what must exist.
                if not source.exists():
                    violations.append(
                        f"{page.relative_to(policy.docs_root)}: link -> {target} names a "
                        f"generated file with no source at {source}"
                    )

                continue

            violations.append(f"{page.relative_to(policy.docs_root)}: dangling link -> {target}")

    return violations


# ----------------------- #


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--pyproject",
        default=Path("pyproject.toml"),
        type=Path,
        help=f"pyproject.toml holding [tool.{_CONFIG_TABLE}] (default: pyproject.toml)",
    )
    args = parser.parse_args(argv)

    policy = load_policy(args.pyproject)
    symbols = discover_symbols()
    corpus = "\n".join(
        path.read_text(encoding="utf-8") for path in policy.docs_root.rglob("*.md")
    )

    symbol_violations, documented = check_symbols(symbols, corpus, policy)
    violations = symbol_violations + check_nav(policy) + check_links(policy)

    exempt_total = sum(len(group.symbols) for group in policy.exempt_groups)
    planes: dict[str, tuple[int, int]] = {}

    for name, symbol in symbols.items():
        covered, total = planes.get(symbol.plane, (0, 0))
        planes[symbol.plane] = (covered + (1 if name in documented else 0), total + 1)

    width = max((len(plane) for plane in planes), default=0)

    for plane in sorted(planes, key=lambda key: (planes[key][0] / planes[key][1], key)):
        covered, total = planes[plane]
        marker = "ok" if covered == total else f"{total - covered} exempt"
        print(f"{plane:<{width}}  {covered:3d}/{total:<3d} documented  {marker}")

    if violations:
        print(f"\nDocs floors FAILED ({len(violations)} violation(s)):")

        for violation in violations:
            print(f"  - {violation}")

        return 1

    print(
        f"\nDocs floors passed: {len(documented)}/{len(symbols)} contract symbol(s) "
        f"documented, {exempt_total} declared exempt."
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
