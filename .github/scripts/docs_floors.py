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
import inspect
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
_DEP_KEY_PATTERN = re.compile(r"\b([A-Z][A-Za-z0-9]*DepKey)\b")
_ACCESSOR_PATTERN = re.compile(r"`ctx\.([a-z_]+)")
_CELL_SPLIT_PATTERN = re.compile(r"(?<!\\)\|")
_LABEL_LINK_PATTERN = re.compile(r"\[([^\]]+)\]\([^)]+\)")
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
    dep_key_index: Path | None = None
    """Docs-root-relative page whose tables map a capability to its dep keys, checked for
    attribution (see :func:`check_dep_key_attribution`). ``None`` skips that check."""
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

    raw_index = table.get("dep_key_index")

    return Policy(
        docs_root=Path(str(table["docs_root"])),
        nav_config=Path(str(table["nav_config"])),
        orphan_allow=tuple(str(pattern) for pattern in table.get("orphan_allow", ())),
        exempt_groups=groups,
        generated=generated,
        dep_key_index=Path(str(raw_index)) if raw_index is not None else None,
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


def accessor_key_owners() -> dict[str, frozenset[str]]:
    """Dep key → the ``ctx.<attr>`` accessors whose ``Deps`` class resolves it.

    Read out of the accessor's own source, so the mapping is the code's rather than a
    second copy of it kept by hand. A key no accessor resolves (the durable family, the
    queue and pub/sub keys, the crypto internals) is simply absent — its attribution is
    not checkable this way, and claiming otherwise would put a guess in a gate.
    """

    from forze.application.execution.context import ExecutionContext

    module = vars(sys.modules[ExecutionContext.__module__])
    owners: dict[str, set[str]] = {}
    declared_by: dict[str, Any] = dict(ExecutionContext.__annotations__)

    # Alias accessors are properties, not annotated fields (`ctx.doc` returns the same
    # `DocumentDeps` as `ctx.document`, and `forze_kits` is written with the short one). A
    # row may legitimately show either, so both have to map to the same keys — otherwise
    # the bar reports a correct row for naming the alias.
    for attribute, value in vars(ExecutionContext).items():
        if isinstance(value, property) and value.fget is not None:
            returns = value.fget.__annotations__.get("return")

            if returns is not None:
                declared_by.setdefault(attribute, returns)

    for attribute, annotation in declared_by.items():
        declared = module.get(annotation) if isinstance(annotation, str) else annotation

        if not isinstance(declared, type):
            continue

        try:
            source = inspect.getsource(declared)
        except (OSError, TypeError):
            # TypeError: a builtin annotation (`bool`, `str`) has no source file; OSError:
            # a class whose module is not on disk. Neither can own a dep key.
            continue

        for key in _DEP_KEY_PATTERN.findall(source):
            owners.setdefault(key, set()).add(attribute)

    return {key: frozenset(attributes) for key, attributes in owners.items()}


def index_rows(page: Path) -> list[tuple[str, frozenset[str], frozenset[str]]]:
    """Each table row of the dep-key index as (capability, keys attributed, accessors shown).

    Cells are split on **unescaped** pipes only: Markdown spells a literal pipe inside a
    cell as ``\\|`` (the reference pages use it for type unions), and splitting on one
    truncates the row — which would drop the key column and silently stop checking the
    attribution it holds. Keys count only from the last cell, so a cross-reference in prose
    or in the accessor cell is not read as an attribution; accessors count from anywhere in
    the row, since some rows name theirs inside the key cell's parenthetical.
    """

    rows: list[tuple[str, frozenset[str], frozenset[str]]] = []

    for line in page.read_text(encoding="utf-8").splitlines():
        if not line.startswith("| ") or "DepKey" not in line:
            continue

        cells = [cell.strip() for cell in _CELL_SPLIT_PATTERN.split(line.strip().strip("|"))]
        capability = _LABEL_LINK_PATTERN.sub(r"\1", cells[0])
        rows.append(
            (
                capability,
                frozenset(_DEP_KEY_PATTERN.findall(cells[-1])),
                frozenset(_ACCESSOR_PATTERN.findall(line)),
            )
        )

    return rows


def check_dep_key_attribution(policy: Policy) -> list[str]:
    """Every dep key the index attributes to a capability is one that row resolves.

    The symbol check above only asks whether a name is *mentioned* somewhere, which a
    table of 75 keys satisfies while attributing any of them to the wrong capability —
    the one defect that makes such an index worse than no index. Here each key named in a
    row is compared against the accessors the row itself shows: if some ``ctx`` accessor
    resolves that key, the row must be one that names it.
    """

    if policy.dep_key_index is None:
        return []

    page = policy.docs_root / policy.dep_key_index

    if not page.is_file():
        return [f"{policy.dep_key_index}: dep-key index page not found"]

    owners = accessor_key_owners()
    violations: list[str] = []

    for capability, keys, shown in index_rows(page):
        for key in keys:
            resolved_by = owners.get(key)

            if resolved_by is None or resolved_by & shown:
                continue

            expected = ", ".join(f"ctx.{name}" for name in sorted(resolved_by))
            violations.append(
                f"{policy.dep_key_index}: row '{capability}' names {key}, which "
                f"{expected} resolves and this row does not — the key is attributed to "
                "the wrong capability, or the row is missing that accessor"
            )

    return violations


def check_dep_key_index_completeness(policy: Policy) -> list[str]:
    """Every accessor-resolved dep key is attributed by the index, or declared exempt.

    The attribution check above asks whether a *named* key sits in the right row, which
    leaves the other direction open: a key dropped from the index goes unnoticed as long as
    some other page still mentions it in prose, and the wiring-facing table quietly stops
    covering its plane. Exemption is the same escape hatch the symbol bar uses — a key
    whose plane has no reference page yet is declared, not pretended.
    """

    if policy.dep_key_index is None:
        return []

    page = policy.docs_root / policy.dep_key_index

    if not page.is_file():
        return []  # the attribution check reports the missing page; one voice is enough

    owners = accessor_key_owners()
    attributed = {key for _, keys, _ in index_rows(page) for key in keys}
    # An accessor family still carrying a declared gap is mid-documentation: the identity
    # lifecycle ports are exempt while their own reference page is unwritten, and demanding
    # them here would move that phase's work into this bar. The scope retires itself — the
    # family becomes subject to completeness as soon as its last exemption goes. It is a
    # narrow escape, not a mute button: every undocumented key still owes the symbol bar a
    # mention or an exemption of its own.
    conceded = {
        accessor
        for key, accessors in owners.items()
        if policy.exempt_for(key) is not None
        for accessor in accessors
    }
    violations = []

    for key, resolved_by in sorted(owners.items()):
        if key in attributed or policy.exempt_for(key) is not None:
            continue

        if resolved_by & conceded:
            continue

        accessors = ", ".join(f"ctx.{name}" for name in sorted(resolved_by))
        violations.append(
            f"{policy.dep_key_index}: no row attributes {key}, which {accessors} "
            "resolves — name it in the row for its capability, or declare it in an "
            "exempt group with the reason its plane is not documented yet"
        )

    return violations


def check_nav(policy: Policy) -> list[str]:
    """Nav entries resolve to files, and every file is reachable from the nav."""

    violations: list[str] = []
    entries = nav_entries(policy.nav_config)
    on_disk = {
        path.relative_to(policy.docs_root).as_posix() for path in policy.docs_root.rglob("*.md")
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
    corpus = "\n".join(path.read_text(encoding="utf-8") for path in policy.docs_root.rglob("*.md"))

    symbol_violations, documented = check_symbols(symbols, corpus, policy)
    violations = (
        symbol_violations
        + check_nav(policy)
        + check_links(policy)
        + check_dep_key_attribution(policy)
        + check_dep_key_index_completeness(policy)
    )

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
