"""The offline gates: syntax, import resolution, structure and links, package census.

Each check reports every problem it finds rather than stopping at the first, because a
corpus-wide gate that fails one item at a time turns a single fix into N review cycles.

None of them keeps a list of failures it has agreed to tolerate. Where a check cannot
answer a question it says so in its output — an unimportable module is printed as a
skip, and the census prints what is not covered — so the residue is visible rather than
recorded in a file that only ever grows.
"""

from __future__ import annotations

import ast
import importlib
import importlib.util
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from .corpus import SKILL_FILENAME, CodeBlock, Corpus, Document

# ----------------------- #

FORZE_ROOT = "forze"
"""The one package name that is a Forze package without carrying the ``forze_`` prefix."""

FRAGMENT_MARKER = "fragment"
"""Info-string token declaring a block is deliberately not a parseable module."""

REQUIRED_SECTIONS = ("Anti-patterns", "Reference")
"""Sections ``skills/AUTHORING.md`` mandates in prose, mechanized here."""

PUBLISHED_DOCS_HOST = "morzecrew.github.io"
PUBLISHED_DOCS_PREFIX = f"https://{PUBLISHED_DOCS_HOST}/forze/latest/"

_ALLOWED_MARKERS = frozenset({FRAGMENT_MARKER})
_INDEX_ROW = re.compile(r"^\|\s*\*\*(?P<name>[a-z0-9-]+)\*\*\s*\|")
PUBLISHED_URL_PATTERN = re.compile(rf"https?://{re.escape(PUBLISHED_DOCS_HOST)}/[^\s)>\"'`]*")


@dataclass
class Result:
    """One check's outcome: what failed, what it could not look at, what it covered."""

    name: str
    violations: list[str] = field(default_factory=list)
    skips: list[str] = field(default_factory=list)
    summary: str = ""

    @property
    def ok(self) -> bool:
        return not self.violations


ResolutionStatus = Literal["ok", "defect", "skip"]
"""Why an import assertion did or did not hold — a defect is the corpus's, a skip this
environment's."""


@dataclass
class _Tally:
    """Import assertions counted by outcome, so the denominator cannot silently shrink."""

    ok: int = 0
    defect: int = 0
    skipped: int = 0

    @property
    def total(self) -> int:
        return self.ok + self.defect + self.skipped

    def record(self, status: ResolutionStatus, count: int) -> None:
        if status == "ok":
            self.ok += count
        elif status == "defect":
            self.defect += count
        else:
            self.skipped += count


def is_forze_module(module: str) -> bool:
    """Whether a dotted module name belongs to this project.

    Matched on a module boundary, never a string prefix: ``startswith("forze")`` would
    also swallow a third-party ``forzex``, silently checking someone else's package
    against this repository's exports.
    """
    root = module.split(".", 1)[0]

    return root == FORZE_ROOT or root.startswith(f"{FORZE_ROOT}_")


# ----------------------- #


def check_syntax(corpus: Corpus) -> Result:
    """Every ``python`` block parses, and every ``fragment`` marker is load-bearing.

    Zero tolerated failures by construction. A block that genuinely cannot stand alone
    is marked at the fence, and a marked block that parses anyway is itself a failure —
    otherwise the marker becomes a way to opt out of the check by sprinkling it.
    """
    result = Result(name="syntax")
    marked = 0
    parsed = 0

    # An empty denominator is not a pass. "0/0 blocks parsed, ok" is what this gate looks
    # like once the extractor stops finding anything — a renamed fence convention, a glob
    # that no longer matches — and it is indistinguishable from a corpus with no examples.
    # The corpus has 127 blocks; zero means the checker broke, not that the corpus is
    # clean. The refusal belongs here, at the seam that knows the denominator, rather than
    # in whichever caller happens to look at the number.
    if corpus.skills and not corpus.python_blocks:
        result.violations.append(
            f"{corpus.root}: {len(corpus.skills)} skill(s) but not one python block — "
            f"the extractor found nothing to check, which is a checker failure, not a "
            f"clean corpus"
        )

    for block in corpus.unclosed_blocks:
        # An unclosed fence silently absorbs the rest of the document, taking its headings
        # and links out of every other check with it.
        result.violations.append(
            f"{block.doc}:{block.line}: fence is never closed — it swallows the rest of "
            f"the file, and every check below it stops seeing anything"
        )

    for block in corpus.python_blocks:
        where = f"{block.doc}:{block.line}"
        unknown = set(block.markers) - _ALLOWED_MARKERS

        if unknown:
            result.violations.append(
                f"{where}: unknown fence marker(s) {sorted(unknown)} — "
                f"only {sorted(_ALLOWED_MARKERS)} is defined"
            )

        parses, error = _parses(block)

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
            result.violations.append(f"{where}: does not parse — {error}")

    unmarked = len(corpus.python_blocks) - marked
    result.summary = f"{parsed}/{unmarked} python block(s) parsed, {marked} marked fragment"

    return result


def check_imports(corpus: Corpus, shipped_packages: frozenset[str]) -> Result:
    """Every ``forze*`` symbol a block imports still exists.

    The load-bearing gate: it is the only mechanical link between the corpus's prose and
    the API that prose describes, and it fails exactly when a rename or a re-export
    removal lands — the change class review is worst at catching, because the reviewer
    is reading ``src/`` while the stale claim sits in Markdown.

    ``ast.Import`` and ``ast.ImportFrom`` are not interchangeable. The first names a
    module and has no symbol to check; the second names a module *and* symbols, each of
    which must be an attribute of it or an importable submodule in its own right.
    """
    result = Result(name="imports")
    tally = _Tally()
    unresolved: dict[str, str] = {}
    distinct: set[tuple[str, str]] = set()

    for block in corpus.python_blocks:
        parses, _ = _parses(block)

        if not parses:
            continue

        for node in ast.walk(ast.parse(block.source)):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    # `import x as y` binds `y` locally and says nothing about upstream;
                    # the name to look up is always `alias.name`.
                    if not is_forze_module(alias.name):
                        continue

                    distinct.add((alias.name, ""))
                    status = _resolve_module(
                        alias.name, block, result, unresolved, shipped_packages
                    )
                    tally.record(status, count=1)

                continue

            if not isinstance(node, ast.ImportFrom):
                continue

            # A relative import names a module in the reader's own application, which
            # this repository knows nothing about and must not claim to check.
            if node.level or node.module is None or not is_forze_module(node.module):
                continue

            module = node.module
            distinct.update((module, alias.name) for alias in node.names)

            if any(alias.name == "*" for alias in node.names):
                result.violations.append(
                    f"{block.doc}:{block.line}: `from {module} import *` cannot be "
                    f"verified — name the symbols explicitly"
                )
                tally.record("defect", count=len(node.names))
                continue

            status = _resolve_module(module, block, result, unresolved, shipped_packages)

            if status != "ok":
                # The module did not resolve, so its symbols were never looked at. They
                # are the module's status, not silently resolved.
                tally.record(status, count=len(node.names))
                continue

            for alias in node.names:
                if _symbol_exists(module, alias.name):
                    tally.record("ok", count=1)
                    continue

                result.violations.append(
                    f"{block.doc}:{block.line}: `{module}` has no attribute or "
                    f"submodule `{alias.name}`"
                )
                tally.record("defect", count=1)

    result.skips = [f"{module}: {reason}" for module, reason in sorted(unresolved.items())]
    result.summary = (
        f"{tally.ok}/{tally.total} forze import(s) resolved "
        f"({len(distinct)} distinct), {tally.skipped} skipped"
    )

    return result


def check_structure(corpus: Corpus) -> Result:
    """Frontmatter, required sections, link integrity, and index parity.

    The escape rule is expressed as *resolves inside ``skills/``* rather than as a list
    of forbidden prefixes. Installed skills are copied out of this repository, so every
    path that leaves the published tree breaks there — which is the whole rationale, and
    a rationale a blocklist can only ever approximate one name at a time.
    """
    result = Result(name="structure")

    if not corpus.skills:
        # Same refusal as the syntax gate's: nothing to check reads exactly like nothing
        # wrong, and only one of those is a claim.
        result.violations.append(f"{corpus.root}: no `*/{SKILL_FILENAME}` found at all")

    for doc in corpus.skills:
        where = str(doc.path)

        if doc.frontmatter is None:
            result.violations.append(f"{where}: no YAML frontmatter")
        else:
            for key in ("name", "description"):
                if not doc.frontmatter.get(key):
                    result.violations.append(f"{where}: frontmatter has no `{key}`")

            declared = doc.frontmatter.get("name")

            if declared and declared != doc.skill_name:
                result.violations.append(
                    f"{where}: frontmatter name `{declared}` != directory `{doc.skill_name}`"
                )

        for section in REQUIRED_SECTIONS:
            if doc.section(section) is None:
                result.violations.append(f"{where}: no `## {section}` section")

        result.violations.extend(_check_reference_section(doc))

    result.violations.extend(_check_version_segment(corpus))

    for doc in corpus.documents:
        for link in doc.links:
            if link.is_external:
                continue

            if link.target.startswith("/"):
                result.violations.append(
                    f"{doc.path}:{link.line}: absolute path link -> {link.target}"
                )
                continue

            if not link.path_part:
                continue

            target = (doc.path.parent / link.path_part).resolve()

            if not target.exists():
                result.violations.append(f"{doc.path}:{link.line}: dangling link -> {link.target}")
                continue

            if doc.is_skill and not target.is_relative_to(corpus.root.resolve()):
                result.violations.append(
                    f"{doc.path}:{link.line}: link escapes the published tree -> "
                    f"{link.target} (installed skills are copied out of this repository)"
                )

    result.violations.extend(_check_index_parity(corpus))
    result.summary = (
        f"{len(corpus.skills)} skill(s), "
        f"{sum(len(doc.links) for doc in corpus.documents)} link(s) checked"
    )

    return result


def check_census(corpus: Corpus, shipped_packages: frozenset[str]) -> Result:
    """Report-only: which shipped packages the corpus actually imports.

    Keyed on wheel packages, never on extras. The two differ by more than naming —
    several extras install submodules of one package, and a couple ship no package at
    all — and it is imports the corpus makes claims about.

    This check never fails. What the number must be, and when it may not regress, is
    somebody else's decision; a coverage gate landing with pre-existing gaps is a gate
    that acquires a tolerated-failure list on day one.
    """
    result = Result(name="census")
    imported: set[str] = set()

    for block in corpus.python_blocks:
        parses, _ = _parses(block)

        if not parses:
            continue

        for node in ast.walk(ast.parse(block.source)):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".", 1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and not node.level and node.module:
                imported.add(node.module.split(".", 1)[0])

    prose = "\n".join(doc.text for doc in corpus.documents)
    buckets: dict[str, list[str]] = {"imported": [], "prose only": [], "absent": []}

    for package in sorted(shipped_packages):
        if package in imported:
            buckets["imported"].append(package)
        elif re.search(rf"\b{re.escape(package)}\b", prose):
            buckets["prose only"].append(package)
        else:
            buckets["absent"].append(package)

    result.summary = ", ".join(f"{len(members)} {label}" for label, members in buckets.items())
    result.skips = [
        f"{label}: {' '.join(members)}"
        for label, members in buckets.items()
        if label != "imported" and members
    ]

    return result


# ----------------------- #


def load_shipped_packages(pyproject: Path) -> frozenset[str]:
    """The wheel's package list — the authoritative set of importable Forze packages."""
    config = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    packages = config["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"]

    return frozenset(Path(package).name for package in packages)


def _parses(block: CodeBlock) -> tuple[bool, str]:
    try:
        ast.parse(block.source)
    except SyntaxError as error:
        return False, f"{error.msg} (line {error.lineno})"

    return True, ""


def _resolve_module(
    module: str,
    block: CodeBlock,
    result: Result,
    unresolved: dict[str, str],
    shipped_packages: frozenset[str],
) -> ResolutionStatus:
    """Import ``module``, classifying a failure as a corpus defect or a harness one.

    A root that is not a shipped package cannot be an environment artifact — no install
    would ever provide it — so it is a corpus defect and fails the gate. A shipped root
    that will not import means this run's environment is missing an extra, which is
    reported as a skip so a shrinking denominator stays visible instead of quietly
    turning the gate green.

    The distinction is what stops the skip path from becoming a hole: a package deleted
    or renamed upstream leaves the wheel's package list too, so it lands in the defect
    branch rather than being written off as a missing extra.
    """
    root = module.split(".", 1)[0]

    if root not in shipped_packages:
        result.violations.append(
            f"{block.doc}:{block.line}: `{module}` names no shipped package "
            f"(`{root}` is not in the wheel)"
        )

        return "defect"

    if root in unresolved or module in unresolved:
        return "skip"

    # Every shipped package lives in one wheel, so a root that will not import means the
    # environment is missing that package's third-party dependencies — an extra this run
    # did not install. Nothing about the corpus can be concluded from it.
    try:
        importlib.import_module(root)
    except Exception as error:
        unresolved[root] = f"{type(error).__name__}: {error}"

        return "skip"

    if module == root:
        return "ok"

    try:
        importlib.import_module(module)
    except ModuleNotFoundError as error:
        # The submodule's own imports may pull a third-party package this environment
        # lacks; that is the harness again. A *forze* name going missing is not — it is
        # the rename this gate exists to catch.
        missing = error.name or ""

        if not missing or not is_forze_module(missing):
            unresolved[module] = f"{type(error).__name__}: {error}"

            return "skip"

        result.violations.append(
            f"{block.doc}:{block.line}: `{module}` does not import — no module named `{missing}`"
        )

        return "defect"
    except Exception as error:
        unresolved[module] = f"{type(error).__name__}: {error}"

        return "skip"

    return "ok"


def _symbol_exists(module: str, name: str) -> bool:
    imported = importlib.import_module(module)

    if hasattr(imported, name):
        return True

    # `from forze_kits import aggregates` names a submodule, which is an attribute only
    # once something has imported it — so absence from the parent proves nothing.
    try:
        return importlib.util.find_spec(f"{module}.{name}") is not None
    except (ImportError, AttributeError, ValueError):
        return False


def _check_version_segment(corpus: Corpus) -> list[str]:
    """Every published-docs URL carries the ``latest`` alias segment.

    Scanned over the raw text rather than over parsed links. The bare form 404s whether
    it was written as a Markdown link or dropped into a sentence, and checking only links
    would leave the prose spelling — the easier one to write by accident — unguarded.
    """
    violations: list[str] = []

    for doc in corpus.documents:
        for number, line in enumerate(doc.text.split("\n"), start=1):
            for match in PUBLISHED_URL_PATTERN.finditer(line):
                url = match.group(0)

                if url.startswith(PUBLISHED_DOCS_PREFIX):
                    continue

                violations.append(
                    f"{doc.path}:{number}: published-docs URL without the `latest` "
                    f"version segment (the bare form 404s) -> {url}"
                )

    return violations


def _check_reference_section(doc: Document) -> list[str]:
    body = doc.section("Reference")

    if body is None:
        return []

    violations: list[str] = []
    note = next((line for line in body.split("\n") if line.strip().startswith(">")), None)

    if note is None or "latest" not in note:
        violations.append(
            f"{doc.path}: `## Reference` does not open with the versioned-docs note "
            f"(a blockquote telling readers to swap `latest` for their pinned minor)"
        )

    if not PUBLISHED_URL_PATTERN.search(body):
        violations.append(f"{doc.path}: `## Reference` cites no published doc URL")

    return violations


def _check_index_parity(corpus: Corpus) -> list[str]:
    """Every skill is in ``skills/README.md``'s table, and every table row is a skill."""
    index = next((doc for doc in corpus.companions if doc.path.name == "README.md"), None)

    if index is None:
        return [f"{corpus.root}: no README.md to check the index against"]

    listed = {
        match.group("name")
        for line in index.text.split("\n")
        if (match := _INDEX_ROW.match(line)) is not None
    }
    present = {doc.skill_name for doc in corpus.skills}

    return [
        f"{index.path}: `{name}` is in the index table but no such skill directory exists"
        for name in sorted(listed - present)
    ] + [
        f"{index.path}: skill `{name}` is missing from the index table"
        for name in sorted(present - listed)
    ]
