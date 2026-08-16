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
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from .corpus import SKILL_FILENAME, CodeBlock, Corpus, Document
from .manifest import NEEDS_RATIONALE, Manifest

# ----------------------- #

FORZE_ROOT = "forze"
"""The one package name that is a Forze package without carrying the ``forze_`` prefix."""

FRAGMENT_MARKER = "fragment"
"""Info-string token declaring a block is deliberately not a parseable module."""

REQUIRED_SECTIONS = ("Reference",)
"""Sections every published file must carry.

``Anti-patterns`` is deliberately **not** here. It was required back when a skill was one
self-contained topic; the corpus now routes each anti-pattern to the reference that owns
its subject, so a file with no mistake of its own legitimately has none, and requiring the
heading everywhere would produce empty sections written to satisfy a check. What the rule
protected — that the corpus states its mistakes — is now held by `check_structure`'s
corpus-level floor instead of by a per-file heading.
"""

PUBLISHED_SKILL = "forze-skills"
"""The one directory under ``skills/`` that holds a ``SKILL.md``.

An installer copies a skill directory recursively and cannot prune a directory it is not
overwriting, so a second skill left behind here becomes a stale copy in every consumer
repository that installs. The post-condition is asserted rather than eyeballed.
"""

PUBLISHED_DOCS_HOST = "morzecrew.github.io"
PUBLISHED_DOCS_PREFIX = f"https://{PUBLISHED_DOCS_HOST}/forze/latest/"

_ALLOWED_MARKERS = frozenset({FRAGMENT_MARKER})
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
                symbol = f"{module}.{alias.name}"
                status = _symbol_exists(module, alias.name)

                if status == "ok":
                    tally.record("ok", count=1)
                    continue

                if status == "skip":
                    # The submodule exists but would not initialize here. Whether that is
                    # this environment's fault or the corpus's cannot be told apart, so it
                    # is reported as unchecked — and unchecked fails, like any other skip.
                    unresolved.setdefault(symbol, _import_status(symbol)[1])
                    tally.record("skip", count=1)
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

    if not corpus.references:
        result.violations.append(
            f"{corpus.root}: the index routes to reference files and none were found — "
            f"the loader saw an index with nothing behind it"
        )

    names = sorted(doc.skill_name for doc in corpus.skills)

    if names not in ([], [PUBLISHED_SKILL]):
        result.violations.append(
            f"{corpus.root}: expected exactly one published skill (`{PUBLISHED_SKILL}`), "
            f"found {names} — an installer cannot prune a directory it is not overwriting, "
            f"so a leftover ships forever"
        )

    for doc in corpus.skills:
        result.violations.extend(_check_skill_shape(doc))
        result.violations.extend(_check_index_note(doc))

    for doc in corpus.references:
        result.violations.extend(_check_required_sections(doc))

    if not any("## Anti-patterns" in doc.text for doc in corpus.references):
        # Replaces the old per-file heading requirement. The corpus must still say what
        # goes wrong; a routing rule that quietly emptied every one of them would
        # otherwise pass every check here.
        result.violations.append(
            f"{corpus.root}: not one reference states an anti-pattern — they are routed by "
            f"subject, not optional in aggregate"
        )

    result.violations.extend(_check_version_segment(corpus))
    result.violations.extend(_check_relative_links(corpus))
    result.violations.extend(_check_index_parity(corpus))
    result.summary = (
        f"{len(corpus.skills)} skill(s), {len(corpus.references)} reference(s), "
        f"{sum(len(doc.links) for doc in corpus.documents)} link(s) checked"
    )

    return result


def _check_skill_shape(doc: Document) -> list[str]:
    """Frontmatter identity and the sections ``AUTHORING.md`` mandates."""
    where = str(doc.path)
    violations: list[str] = []

    if doc.frontmatter is None:
        violations.append(f"{where}: no YAML frontmatter")
    else:
        violations.extend(
            f"{where}: frontmatter has no `{key}`"
            for key in ("name", "description")
            if not doc.frontmatter.get(key)
        )
        declared = doc.frontmatter.get("name")

        if declared and declared != doc.skill_name:
            violations.append(
                f"{where}: frontmatter name `{declared}` != directory `{doc.skill_name}`"
            )

    violations.extend(
        f"{where}: no `## {section}` section"
        for section in REQUIRED_SECTIONS
        if doc.section(section) is None
    )

    return violations


def _check_relative_links(corpus: Corpus) -> list[str]:
    """Every relative link resolves, and none of a skill's leaves the tree that ships."""
    violations: list[str] = []
    # The boundary is the *skill directory*, not the corpus root. An install copies one
    # skill directory; `skills/README.md` and `skills/AUTHORING.md` sit beside it and stay
    # here. Drawing the line at the root accepts a link to those two — resolvable in this
    # repository, dangling the moment the skill is installed anywhere else.
    homes = tuple(sorted({doc.path.parent.resolve() for doc in corpus.skills}))
    # Reference files ship exactly as the index does — the installer copies the directory
    # recursively — so the rule is about being published, not about being a `SKILL.md`.
    # Keying it on the filename would leave 43 of 44 files unguarded.
    ships = {
        doc.path: home
        for doc in corpus.published
        for home in homes
        if doc.path.resolve().is_relative_to(home)
    }

    for doc in corpus.documents:
        for link in doc.links:
            if link.is_external or not link.path_part:
                continue

            if link.target.startswith("/"):
                violations.append(f"{doc.path}:{link.line}: absolute path link -> {link.target}")
                continue

            target = (doc.path.parent / link.path_part).resolve()

            if not target.exists():
                violations.append(f"{doc.path}:{link.line}: dangling link -> {link.target}")
                continue

            home = ships.get(doc.path)

            if home is not None and not target.is_relative_to(home):
                violations.append(
                    f"{doc.path}:{link.line}: link escapes the published tree -> "
                    f"{link.target} (installed skills are copied out of this repository)"
                )

    return violations


def imported_units(corpus: Corpus) -> set[str]:
    """Every module path the corpus imports, and each of its ancestors.

    ``from forze_kms.aws import AwsKmsClient`` covers ``forze_kms.aws`` *and* ``forze_kms``:
    the root is genuinely demonstrated by a submodule's import. The reverse does not hold,
    which is the asymmetry the whole unit rule rests on — importing ``forze_kms.aws`` says
    nothing about ``forze_kms.gcp``, and a package-keyed census that scored the root green
    reported exactly that.
    """
    reached: set[str] = set()

    for block in corpus.python_blocks:
        parses, _ = _parses(block)

        if not parses:
            continue

        for node in ast.walk(ast.parse(block.source)):
            if isinstance(node, ast.Import):
                reached.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and not node.level and node.module:
                reached.add(node.module)
                # `from forze_kms import gcp` imports a *submodule*, and the import gate
                # resolves it as one. Recording only `forze_kms` would leave the census
                # calling `forze_kms.gcp` unproven while the gate beside it reports the
                # same line resolved — two checks disagreeing about one import.
                reached.update(f"{node.module}.{alias.name}" for alias in node.names)

    covered: set[str] = set()

    for module in reached:
        parts = module.split(".")
        covered.update(".".join(parts[: index + 1]) for index in range(len(parts)))

    return {module for module in covered if module.split(".")[0].startswith("forze")}


def check_census(corpus: Corpus, manifest: Manifest) -> Result:
    """Every unit carries a doctrine, and every D1/D2 unit is reached by a resolved import.

    **Consumption, not declaration.** A unit counts as covered only when a symbol from it
    appears in a code block the import gate actually resolves. Naming it in prose, in a
    table, or in a frontmatter description does not count — that is precisely the condition
    the corpus was in when this was written, and precisely what a declaration-based census
    would have scored green.

    The manifest's own violations are reported here rather than separately: an unclassified
    extra and an unproven unit are the same failure at different distances from the corpus,
    and splitting them across two checks lets a reader fix one and think they are done.
    """
    result = Result(name="census")
    result.violations.extend(manifest.violations)

    covered = imported_units(corpus)
    prose = "\n".join(doc.text for doc in corpus.documents)

    if not manifest.violations and not manifest.proven:
        # A ratchet with nothing to prove is not a ratchet. Every unit sitting in D3 or D4
        # would leave "0/0 proven" reading exactly like full coverage — the zero-denominator
        # pass this checker refuses everywhere else.
        result.violations.append(
            "no unit carries D1 or D2, so the census proves nothing — a corpus where every "
            "shipped package is out of scope or deferred is a decision worth making loudly"
        )

    unproven = [unit for unit in manifest.proven if unit.name not in covered]
    result.violations.extend(
        f"{unit.name}: {unit.doctrine} requires an import the gate resolves, and the corpus "
        f"has {'only prose' if re.search(rf'\b{re.escape(unit.name)}\b', prose) else 'nothing'}"
        for unit in sorted(unproven, key=lambda unit: unit.name)
    )

    by_doctrine: dict[str, int] = {}

    for unit in manifest.units:
        by_doctrine[unit.doctrine] = by_doctrine.get(unit.doctrine, 0) + 1

    proven = len(manifest.proven)
    # A manifest that failed to validate is reported *as* the headline rather than behind a
    # coverage ratio, because the ratio is computed from it: "37/37 proven" above a broken
    # unit list is a number describing a denominator nobody should trust yet.
    counted = (
        f"{proven - len(unproven)}/{proven} D1+D2 unit(s) proven, "
        + ", ".join(f"{count} {doctrine}" for doctrine, count in sorted(by_doctrine.items()))
    )
    result.summary = (
        f"{len(manifest.violations)} manifest problem(s) — the unit list is not trustworthy"
        if manifest.violations
        else counted
    )
    result.skips = [
        f"{unit.doctrine} by decision: {unit.name} — {unit.rationale}"
        for unit in sorted(manifest.units, key=lambda unit: unit.name)
        if unit.doctrine in NEEDS_RATIONALE
    ]

    return result


# ----------------------- #


def load_extras(pyproject: Path) -> frozenset[str]:
    """The declared extras — the repository's own record of where an author makes a choice.

    Derived, never hand-maintained, for the same reason the package list is: an integration
    nobody adds to a hand-written list is an integration the census cannot see.

    A project with no extras at all is a valid project, so a missing table is an empty set
    rather than an error. That is not a hole: with no extras the manifest's own rows become
    "`kms-aws` is not an extra in pyproject.toml", which fails loudly. Raising here instead
    would report the *package* list as unreadable when the package list is fine.
    """
    config = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    project = config.get("project", {})

    if not isinstance(project, dict):
        raise TypeError(f"[project] must be a table, not {type(project).__name__}")

    declared = project.get("optional-dependencies", {})

    if not isinstance(declared, dict):
        raise TypeError(
            f"[project.optional-dependencies] must be a table, not {type(declared).__name__}"
        )

    return frozenset(declared)


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

    # The submodule's own imports may pull a third-party package this environment lacks;
    # that is the harness again. A *forze* name going missing is not — it is the rename
    # this gate exists to catch.
    status, reason = _import_status(module)

    if status == "defect":
        result.violations.append(f"{block.doc}:{block.line}: `{module}` does not import — {reason}")
    elif status == "skip":
        unresolved[module] = reason

    return status


def _symbol_exists(module: str, name: str) -> ResolutionStatus:
    """Whether ``from <module> import <name>`` would actually bind something.

    The submodule branch **imports** rather than locating a spec. ``find_spec`` answers
    "is there a file for this?", which is a weaker question than the corpus's line asks:
    a submodule that exists on disk and raises during initialization has a spec and no
    binding, so a spec-based check reports the example as fine while the reader's copy of
    it fails. Running the same import the corpus does is the only thing that agrees with
    the corpus by construction.

    Failures are classified exactly as ``_resolve_module`` classifies them, so importing
    for real does not turn a missing third-party extra into a corpus defect.
    """
    imported = importlib.import_module(module)

    if hasattr(imported, name):
        return "ok"

    # `from forze_kits import aggregates` names a submodule, which is an attribute of its
    # parent only once something has imported it — so absence from the parent proves
    # nothing until the import is tried.
    return _import_status(f"{module}.{name}")[0]


def _import_status(module: str) -> tuple[ResolutionStatus, str]:
    """Import ``module``, separating a corpus defect from an environment artifact.

    A `forze*` name going missing is the rename this gate exists to catch. A third-party
    name going missing is an extra this run did not install, and says nothing about the
    corpus. Anything else cannot be attributed, so it is reported rather than claimed.
    """
    try:
        importlib.import_module(module)
    except ModuleNotFoundError as error:
        missing = error.name or ""

        if missing and is_forze_module(missing):
            return "defect", f"no module named `{missing}`"

        return "skip", f"{type(error).__name__}: {error}"
    except Exception as error:
        return "skip", f"{type(error).__name__}: {error}"

    return "ok", ""


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


def _check_required_sections(doc: Document) -> list[str]:
    """A reference carries the sections REQUIRED_SECTIONS names, and cites somewhere to go."""
    violations = [
        f"{doc.path}: no `## {section}` section"
        for section in REQUIRED_SECTIONS
        if doc.section(section) is None
    ]
    body = doc.section("Reference")

    if body is not None and not PUBLISHED_URL_PATTERN.search(body) and "](" not in body:
        violations.append(
            f"{doc.path}: `## Reference` points nowhere — cite a published doc URL or a "
            f"sibling reference"
        )

    return violations


def _check_index_note(doc: Document) -> list[str]:
    """The index carries the versioned-docs note, once, for the whole corpus.

    It used to be repeated in every skill. Consolidating it is the point of having an
    index — and it is also how the note stops being 21 copies that can disagree.
    """
    body = doc.section("Reference")

    if body is None:
        return []

    note = next((line for line in body.split("\n") if line.strip().startswith(">")), None)

    if note is None or "latest" not in note:
        return [
            (
                f"{doc.path}: `## Reference` does not open with the versioned-docs note "
                f"(a blockquote telling readers to swap `latest` for their pinned minor)"
            )
        ]

    return []


def _check_index_parity(corpus: Corpus) -> list[str]:
    """Index ↔ reference parity, both directions.

    This is the check the consolidated structure cannot live without: an index is only
    navigation, so a reference nothing links to is unreachable material that still ships,
    and an index row with no file is a dead end the reader finds at the worst moment.
    Neither is visible in a diff that only adds files.
    """
    violations: list[str] = []

    for index in corpus.skills:
        home = index.path.parent.resolve()
        # Keyed on the path relative to the skill directory, not on the filename stem, and
        # matched at any depth rather than one level down. The loader walks recursively, so
        # a reference in a subdirectory ships; comparing stems one level deep left it in no
        # set at all — routed by nothing, reported by nothing, and installed anyway.
        listed = {
            resolved.relative_to(home).as_posix()
            for link in index.links
            if not link.is_external and link.path_part.endswith(".md")
            if (resolved := (index.path.parent / link.path_part).resolve()).is_relative_to(home)
        }
        present = {
            doc.path.resolve().relative_to(home).as_posix()
            for doc in corpus.references
            if doc.path.resolve().is_relative_to(home)
        }

        violations += [
            f"{index.path}: routes to `{name}` but no such reference file exists"
            for name in sorted(listed - present)
        ] + [
            f"{index.path}: reference `{name}` exists but the index routes to nothing"
            for name in sorted(present - listed)
        ]

    return violations
