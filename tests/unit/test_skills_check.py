"""Every skills-corpus gate, seen red.

A gate is not proven by reading it. Each check here is driven by *injecting* the
regression it exists to catch — a renamed export, a broken link, a defensive fragment
marker, a link that escapes the published tree — and asserting the check reports that
specific thing. A test that only asserts the real corpus is green would pass just as
happily against a checker that returns "ok" unconditionally.

The corpus under test is synthetic and minimal, built in `tmp_path`, so a test says what
it is about rather than depending on which skill happens to contain which example today.
Import resolution is the exception and is checked against the *really installed*
packages: it is the one gate whose whole value is the link to the live API, and a mocked
importer would prove only that the mock agrees with itself.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tools.skills_check.__main__ import _run_liveness, main
from tools.skills_check.checks import (
    check_census,
    check_imports,
    check_structure,
    check_syntax,
    is_forze_module,
    load_extras,
    load_shipped_packages,
)
from tools.skills_check.corpus import load_corpus
from tools.skills_check.manifest import Manifest, default_manifest_path, load_manifest
from tools.skills_check.links import (
    LinkOutcome,
    LinkPolicy,
    _fetch,
    check_liveness,
    collect_published_urls,
)

pytestmark = pytest.mark.unit

# ----------------------- #

_REPO = Path(__file__).resolve().parents[2]
"""Anchored on this file, never on the working directory — the suite must say the same
thing whichever directory pytest was invoked from, as every sibling guard test does."""

SHIPPED = frozenset({"forze", "forze_kits", "forze_mock", "forze_postgres", "forze_gone"})

_REFERENCE = """\
## Reference

> Docs are versioned. These links use `latest` (the newest release). If your app pins an
> older `forze` minor, replace `latest` in the URL with that version.

- [Wiring](https://morzecrew.github.io/forze/latest/writing-operation/wiring/)
"""


SKILL_DIR = "forze-skills"


def _skill(
    name: str = SKILL_DIR, routes: tuple[str, ...] = ("demo",), note: str = _REFERENCE
) -> str:
    """Assemble the routing index.

    Built by concatenation rather than by dedenting an interpolated template: a
    multi-line body starting at column zero makes the common prefix empty, so the dedent
    silently does nothing and every line — the frontmatter delimiter included — stays
    indented. The document then fails checks for reasons unrelated to the test.
    """
    rows = "\n".join(f"| [{r}](references/{r}.md) | A demo reference. |" for r in routes)

    return (
        "---\n"
        f"name: {name}\n"
        "description: >-\n"
        "  A demo skill. Use when testing the corpus gates.\n"
        "---\n"
        "\n"
        "# Demo\n"
        "\n"
        "| Reference | Covers |\n"
        "|---|---|\n"
        f"{rows}\n"
        "\n"
        f"{note}"
    )


def _reference(body: str, anti: bool = True) -> str:
    tail = "\n\n## Anti-patterns\n\n- Doing the thing the wrong way." if anti else ""

    return (
        f"# Demo reference\n\n{body}{tail}\n\n## Reference\n\n"
        "- [Wiring](https://morzecrew.github.io/forze/latest/writing-operation/wiring/)\n"
    )


_BASELINE_BODY = "```python\nfrom forze.base.exceptions import CoreException\n```"


@pytest.fixture
def corpus_root(tmp_path: Path) -> Path:
    """A minimal corpus in the consolidated shape, ready to be broken one way at a time.

    One published skill whose `SKILL.md` routes to `references/`, which is where the
    material — and therefore nearly every check — actually lives. The reference carries a
    real python block on purpose: a corpus with none is itself a failure, so a baseline
    without one would be asserting the wrong green.
    """
    root = tmp_path / "skills"
    (root / SKILL_DIR / "references").mkdir(parents=True)
    (root / SKILL_DIR / "SKILL.md").write_text(_skill(), encoding="utf-8")
    (root / SKILL_DIR / "references" / "demo.md").write_text(
        _reference(_BASELINE_BODY), encoding="utf-8"
    )
    (root / "README.md").write_text("# Skills\n\nInstall it.\n", encoding="utf-8")

    return root


def _write(root: Path, body: str = _BASELINE_BODY, anti: bool = True) -> None:
    """Replace the reference's body — where the material under test lives."""
    (root / SKILL_DIR / "references" / "demo.md").write_text(
        _reference(body, anti=anti), encoding="utf-8"
    )


def _write_index(root: Path, **kwargs: object) -> None:
    (root / SKILL_DIR / "SKILL.md").write_text(_skill(**kwargs), encoding="utf-8")  # type: ignore[arg-type]


def _violations(root: Path, check: str) -> list[str]:
    corpus = load_corpus(root)
    result = {
        "syntax": lambda: check_syntax(corpus),
        "imports": lambda: check_imports(corpus, SHIPPED),
        "structure": lambda: check_structure(corpus),
    }[check]()

    return result.violations


# ----------------------- #
# The baseline: an unbroken corpus is green on every gate.


def test_clean_corpus_passes_every_check(corpus_root: Path) -> None:
    for check in ("syntax", "imports", "structure"):
        assert _violations(corpus_root, check) == [], check


# ----------------------- #
# Syntax (§3.2) — zero tolerated failures, and a marker that cannot be sprinkled.


def test_unparseable_block_is_reported(corpus_root: Path) -> None:
    _write(corpus_root, "```python\ndef broken(:\n```")

    violations = _violations(corpus_root, "syntax")

    assert len(violations) == 1
    assert "does not parse" in violations[0]


def test_marked_fragment_that_parses_is_itself_a_failure(corpus_root: Path) -> None:
    """The anti-defensive rule: a marker on a healthy block is the marker being abused."""
    _write(corpus_root, "```python fragment\nx = 1\n```")

    violations = _violations(corpus_root, "syntax")

    assert len(violations) == 1
    assert "drop the marker" in violations[0]


def test_marked_fragment_that_cannot_parse_is_accepted(corpus_root: Path) -> None:
    _write(corpus_root, "```python fragment\n    indented_continuation=1,\n```")

    assert _violations(corpus_root, "syntax") == []


def test_unknown_fence_marker_is_rejected(corpus_root: Path) -> None:
    _write(corpus_root, "```python skip\nx = (\n```")

    violations = _violations(corpus_root, "syntax")

    assert any("unknown fence marker" in violation for violation in violations)


def test_indented_fence_is_dedented_before_parsing(corpus_root: Path) -> None:
    """The extractor bug that made a healthy corpus look like it had a known failure.

    A fence nested in a list item carries that indentation into its body. Parsed as
    written it raises `IndentationError` — a defect of the extractor that reads exactly
    like a defect of the corpus, and the shape a tolerated-failure list grows from.
    """
    _write(corpus_root, "- A list item:\n\n  ```python\n  x = 1\n  ```\n")

    assert _violations(corpus_root, "syntax") == []


@pytest.mark.parametrize("fence", ["python", "py", "python3"])
def test_every_python_spelling_is_checked(corpus_root: Path, fence: str) -> None:
    """A ` ```py ` block is Python to every reader; a gate that skips it checks less
    than its denominator claims."""
    _write(corpus_root, f"```{fence}\ndef broken(:\n```")

    violations = _violations(corpus_root, "syntax")

    assert len(violations) == 1
    assert "does not parse" in violations[0]


def test_a_corpus_with_no_python_blocks_is_a_failure(corpus_root: Path) -> None:
    """The vacuous pass: "0/0 parsed, ok" is what a broken extractor looks like.

    A corpus with skills in it and not one python block means nothing found them, which
    is indistinguishable from a clean run unless the empty case is refused outright.
    """
    _write(corpus_root, "No examples here at all.")

    violations = _violations(corpus_root, "syntax")

    assert len(violations) == 1
    assert "not one python block" in violations[0]


def test_a_corpus_with_no_skills_is_a_failure(tmp_path: Path) -> None:
    root = tmp_path / "skills"
    root.mkdir()
    (root / "README.md").write_text("# Skills\n", encoding="utf-8")

    violations = _violations(root, "structure")

    assert any("no `*/SKILL.md` found at all" in violation for violation in violations)


def test_an_unclosed_fence_is_reported(corpus_root: Path) -> None:
    """It swallows the rest of the file, taking every heading and link below it."""
    _write(corpus_root, "```python\nx = 1")

    violations = _violations(corpus_root, "syntax")

    assert any("never closed" in violation for violation in violations)


# ----------------------- #
# Imports (§3.1) — the load-bearing gate, against the really installed packages.


def test_resolvable_symbols_pass(corpus_root: Path) -> None:
    _write(corpus_root, "```python\nfrom forze.base.exceptions import CoreException\n```")

    assert _violations(corpus_root, "imports") == []


def test_renamed_export_is_caught(corpus_root: Path) -> None:
    """The change class this whole RFC pays for: a symbol that no longer exists."""
    _write(corpus_root, "```python\nfrom forze.base.exceptions import NoSuchThing\n```")

    violations = _violations(corpus_root, "imports")

    assert len(violations) == 1
    assert "has no attribute or submodule `NoSuchThing`" in violations[0]


def test_submodule_import_resolves_without_being_an_attribute(corpus_root: Path) -> None:
    """`from pkg import submodule` names a module, which the parent may not expose."""
    _write(corpus_root, "```python\nfrom forze_kits import aggregates\n```")

    assert _violations(corpus_root, "imports") == []


def test_module_that_names_no_shipped_package_is_a_defect(corpus_root: Path) -> None:
    _write(corpus_root, "```python\nfrom forze_invented import Thing\n```")

    violations = _violations(corpus_root, "imports")

    assert len(violations) == 1
    assert "names no shipped package" in violations[0]


def test_missing_forze_submodule_is_a_defect_not_a_skip(corpus_root: Path) -> None:
    """The hole the skip path would otherwise open.

    A shipped root that will not import is an environment artifact. A *submodule* of a
    root that imports fine is not — nothing about an install makes `forze.no_such_module`
    appear — so it must fail rather than be written off as a missing extra.
    """
    _write(corpus_root, "```python\nfrom forze.no_such_module import Thing\n```")

    violations = _violations(corpus_root, "imports")

    assert len(violations) == 1
    assert "does not import" in violations[0]


def test_submodule_that_cannot_initialise_is_not_reported_as_resolved(
    corpus_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`find_spec` answers a weaker question than the corpus's line asks.

    A submodule that exists on disk and raises during initialization has a spec and no
    binding, so a spec-based check calls the example fine while the reader's copy of it
    raises. The check has to run the same import the corpus does.
    """
    (tmp_path / "forze_probe").mkdir()
    (tmp_path / "forze_probe" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "forze_probe" / "broken.py").write_text(
        "raise RuntimeError('this submodule cannot initialise')\n", encoding="utf-8"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    _write(corpus_root, "```python\nfrom forze_probe import broken\n```")

    result = check_imports(load_corpus(corpus_root), SHIPPED | {"forze_probe"})

    assert result.ok, "unattributable init failures are skips, not corpus defects"
    assert result.skips, "but they are never counted as resolved"
    assert "1 skipped" in result.summary
    assert "0/1" in result.summary


def test_star_import_is_rejected(corpus_root: Path) -> None:
    _write(corpus_root, "```python\nfrom forze.base.exceptions import *\n```")

    violations = _violations(corpus_root, "imports")

    assert len(violations) == 1
    assert "cannot be verified" in violations[0]


def test_alias_binds_the_local_name_and_the_source_is_checked(corpus_root: Path) -> None:
    """Checking `asname` would look up a symbol that by definition does not exist."""
    _write(
        corpus_root,
        "```python\nfrom forze.base.exceptions import NoSuchThing as CoreException\n```",
    )

    violations = _violations(corpus_root, "imports")

    assert len(violations) == 1
    assert "`NoSuchThing`" in violations[0]


def test_plain_module_import_checks_the_module_not_a_symbol(corpus_root: Path) -> None:
    _write(corpus_root, "```python\nimport forze_mock as mock_backend\n```")

    assert _violations(corpus_root, "imports") == []


def test_relative_and_foreign_imports_are_left_alone(corpus_root: Path) -> None:
    """A reader's own package and a third party's are not this repository's to check."""
    _write(
        corpus_root,
        "```python\nfrom .models import Order\nfrom forzex.client import Thing\n```",
    )

    assert _violations(corpus_root, "imports") == []


@pytest.mark.parametrize(
    ("module", "expected"),
    [
        ("forze", True),
        ("forze.base", True),
        ("forze_postgres", True),
        ("forzex", False),
        ("forzex.client", False),
        ("pydantic", False),
    ],
)
def test_forze_membership_is_a_module_boundary(module: str, expected: bool) -> None:
    assert is_forze_module(module) is expected


def test_skipped_module_is_never_counted_as_resolved(corpus_root: Path) -> None:
    """A shrinking denominator is the vacuous pass this gate must not be able to have."""
    _write(corpus_root, "```python\nfrom forze_gone import Thing\n```")

    corpus = load_corpus(corpus_root)
    result = check_imports(corpus, SHIPPED)

    assert result.violations == []
    assert result.skips, "an unimportable shipped package must be reported, not passed over"
    assert "1 skipped" in result.summary


# ----------------------- #
# Structure, links and index parity (§3.3).


def test_missing_required_section_is_reported(corpus_root: Path) -> None:
    path = corpus_root / SKILL_DIR / "references" / "demo.md"
    path.write_text(path.read_text(encoding="utf-8").replace("## Reference", "## Notes"))

    violations = _violations(corpus_root, "structure")

    assert len(violations) == 1
    assert "no `## Reference` section" in violations[0]


def test_a_reference_without_its_own_anti_pattern_is_fine(corpus_root: Path) -> None:
    """Anti-patterns are routed by subject, so a file with no mistake of its own has none.

    The corpus-level floor below is what stops that from emptying every file.
    """
    _write(corpus_root, anti=True)
    (corpus_root / SKILL_DIR / "references" / "extra.md").write_text(
        _reference("Nothing goes wrong here.", anti=False), encoding="utf-8"
    )
    _write_index(corpus_root, routes=("demo", "extra"))

    assert _violations(corpus_root, "structure") == []


def test_a_corpus_that_states_no_anti_pattern_at_all_is_a_failure(corpus_root: Path) -> None:
    _write(corpus_root, anti=False)

    violations = _violations(corpus_root, "structure")

    assert any("not one reference states an anti-pattern" in v for v in violations)


def test_frontmatter_name_must_match_the_directory(corpus_root: Path) -> None:
    _write_index(corpus_root, name="forze-renamed")

    violations = _violations(corpus_root, "structure")

    assert any("!= directory" in violation for violation in violations)


def test_dangling_relative_link_is_reported(corpus_root: Path) -> None:
    _write(corpus_root, "See [siblings](../forze-absent/SKILL.md).")

    violations = _violations(corpus_root, "structure")

    assert len(violations) == 1
    assert "dangling link" in violations[0]


def test_link_escaping_the_published_tree_is_reported(corpus_root: Path, tmp_path: Path) -> None:
    """Installed skills are copied out of this repository, so such a path breaks there."""
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "forze.py").write_text("", encoding="utf-8")
    _write(corpus_root, "See [the source](../../../src/forze.py).")

    violations = _violations(corpus_root, "structure")

    assert len(violations) == 1
    assert "escapes the published tree" in violations[0]


def test_links_inside_code_blocks_are_not_read_as_links(corpus_root: Path) -> None:
    """`deps["widget"]` is not a dangling link, and reporting it trains people to ignore."""
    _write(corpus_root, '```python\nclient = deps["widget"](timeout=1)\n```')

    assert _violations(corpus_root, "structure") == []


@pytest.mark.parametrize(
    "body",
    [
        "See [wiring](https://morzecrew.github.io/forze/writing-operation/).",
        "See https://morzecrew.github.io/forze/writing-operation/ for details.",
    ],
    ids=["markdown-link", "bare-prose-url"],
)
def test_published_link_without_the_latest_segment_is_reported(
    corpus_root: Path, body: str
) -> None:
    """The bare form 404s however it was written — checking only links leaves the
    easier-to-write spelling unguarded."""
    _write(corpus_root, body)

    violations = _violations(corpus_root, "structure")

    assert any("`latest` version segment" in violation for violation in violations)


def test_required_section_at_the_wrong_heading_level_does_not_count(corpus_root: Path) -> None:
    """`#### Reference` nests under whatever precedes it — it is not the section."""
    path = corpus_root / SKILL_DIR / "references" / "demo.md"
    path.write_text(
        path.read_text(encoding="utf-8").replace("## Reference", "#### Reference"),
        encoding="utf-8",
    )

    violations = _violations(corpus_root, "structure")

    assert any("no `## Reference` section" in violation for violation in violations)


def test_index_without_the_versioned_note_is_reported(corpus_root: Path) -> None:
    """The note lives on the index once, for the whole corpus, instead of in every file."""
    _write_index(
        corpus_root, note="## Reference\n\n- [Docs](https://morzecrew.github.io/forze/latest/)\n"
    )

    violations = _violations(corpus_root, "structure")

    assert any("versioned-docs note" in violation for violation in violations)


def test_index_parity_is_checked_in_both_directions(corpus_root: Path) -> None:
    """An unrouted reference ships unreachable; a routed absence is a dead end."""
    _write_index(corpus_root, routes=("ghost",))

    violations = _violations(corpus_root, "structure")

    assert any("no such reference file exists" in violation for violation in violations)
    assert any("the index routes to nothing" in violation for violation in violations)


def test_parity_sees_a_reference_nested_below_the_top_level(corpus_root: Path) -> None:
    """The loader walks recursively, so parity has to as well.

    Matching one level down put a nested file in neither set: routed by nothing, reported
    by nothing, and copied out by an installer that recurses. Every other check passes it,
    which is what made it invisible — this file satisfies the section rule and carries no
    relative links to break at the deeper path.
    """
    nested = corpus_root / SKILL_DIR / "references" / "nested"
    nested.mkdir()
    (nested / "orphan.md").write_text(_reference("Nothing routes here."), encoding="utf-8")

    violations = _violations(corpus_root, "structure")

    assert any("nested/orphan.md` exists but the index routes to nothing" in v for v in violations)


def test_link_to_a_corpus_file_outside_the_skill_directory_is_reported(corpus_root: Path) -> None:
    """`skills/README.md` resolves here and is absent wherever the skill is installed.

    The boundary is the skill directory, not the corpus root: an install copies
    `forze-skills/` and leaves its siblings behind, so a link that merely stays under
    `skills/` is still a link that dangles for every consumer.
    """
    _write(corpus_root, "See [the corpus readme](../../README.md).")

    violations = _violations(corpus_root, "structure")

    assert len(violations) == 1
    assert "escapes the published tree" in violations[0]


# ----------------------- #
# Coverage ratchet — every unit triaged, every D1/D2 unit proven by a resolved import.

_MANIFEST_PACKAGES = frozenset({"forze", "forze_mock"})
_MANIFEST_EXTRAS = frozenset({"mock-server", "postgres"})


def _manifest(
    tmp_path: Path,
    units: str = '"forze" = { doctrine = "D1" }\n"forze_mock" = { doctrine = "D1" }\n'
    '"forze_mock.server" = { doctrine = "D2" }\n',
    subdivides: str = '"mock-server" = "forze_mock.server"\n',
    whole: str = '["postgres"]',
    dependency_only: str = "[]",
) -> Path:
    path = tmp_path / "coverage.toml"
    path.write_text(
        "[extras.subdivides]\n"
        f"{subdivides}\n"
        "[extras.whole-package]\n"
        f"names = {whole}\n\n"
        "[extras.dependency-only]\n"
        f"names = {dependency_only}\n\n"
        "[units]\n"
        f"{units}",
        encoding="utf-8",
    )

    return path


def _loaded(
    tmp_path: Path,
    packages: frozenset[str] = _MANIFEST_PACKAGES,
    extras: frozenset[str] = _MANIFEST_EXTRAS,
    **kwargs: str,
) -> Manifest:
    return load_manifest(_manifest(tmp_path, **kwargs), packages, extras)


def test_a_manifest_that_matches_its_inputs_loads_clean(tmp_path: Path) -> None:
    assert _loaded(tmp_path).violations == []


def test_a_new_package_fails_until_someone_writes_down_a_doctrine(tmp_path: Path) -> None:
    """§7's injected regression, first half: a stub package added to the wheel targets.

    This is the whole mechanism — a new plane must not be able to ship with zero corpus
    reach *and* zero decision. The failure is about the missing decision, not the missing
    coverage: writing `D3, maintainer tooling` is a perfectly good way to make it pass.
    """
    manifest = _loaded(tmp_path, packages=_MANIFEST_PACKAGES | {"forze_opensearch"})

    assert any(
        "forze_opensearch` has no doctrine" in violation for violation in manifest.violations
    )


def test_a_new_extra_subdividing_a_covered_package_fails_too(tmp_path: Path) -> None:
    """§7's injected regression, second half — the one a naive implementation passes.

    `kms-azure` adds a census unit **without adding a package**, and the package it
    subdivides is already green, so a wheel-targets-only ratchet sees nothing at all. That
    is the conformance census's own lesson wearing new clothes: counting at the wrong
    granularity reports green on an unseeded plane.
    """
    manifest = _loaded(tmp_path, extras=_MANIFEST_EXTRAS | {"kms-azure"})

    assert any("extra `kms-azure` is in no table" in v for v in manifest.violations)
    assert not any("forze_kms.azure" in v for v in manifest.violations), (
        "the checker must not invent a submodule name from an extra name"
    )


def test_a_mapped_unit_that_does_not_import_is_refused(tmp_path: Path) -> None:
    """Rule 2 — otherwise a renamed module silently drops a unit from the denominator."""
    manifest = _loaded(tmp_path, subdivides='"mock-server" = "forze_mock.gone"\n')

    assert any("does not import" in violation for violation in manifest.violations)


def test_a_doctrine_row_for_something_that_is_not_a_unit_is_refused(tmp_path: Path) -> None:
    manifest = _loaded(tmp_path, units='"forze" = { doctrine = "D1" }\n"nope" = { doctrine = "D1" }\n')

    assert any("`nope` carries a doctrine but is not a census unit" in v for v in manifest.violations)


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ('"forze" = { doctrine = "D9" }\n', "one of D1, D2, D3, D4"),
        ('"forze" = { doctrine = "D3" }\n', "carries no rationale"),
        ('"forze" = { doctrine = "D4", rationale = "later" }\n', "carries no trigger"),
    ],
    ids=["unknown-doctrine", "d3-without-rationale", "d4-without-trigger"],
)
def test_a_doctrine_must_carry_what_it_promises(tmp_path: Path, row: str, expected: str) -> None:
    manifest = _loaded(tmp_path, units=row)

    assert any(expected in violation for violation in manifest.violations)


def test_a_d2_unit_named_only_in_prose_does_not_count(corpus_root: Path, tmp_path: Path) -> None:
    """Consumption, not declaration — the condition the corpus was in when this was written.

    D1 and D2 differ in how much surrounding material is expected, never in whether the
    import is verified. A D2 anchor satisfied by a sentence would be the tolerated-failure
    list this gate refuses to grow.
    """
    _write(corpus_root, "Wire it with `forze_mock.server`, which is a real module.")
    manifest = _loaded(tmp_path)

    result = check_census(load_corpus(corpus_root), manifest)

    assert not result.ok
    assert any(
        "forze_mock.server: D2 requires an import" in violation and "only prose" in violation
        for violation in result.violations
    )


def test_importing_a_submodule_proves_its_root_but_not_its_siblings(
    corpus_root: Path, tmp_path: Path
) -> None:
    """The asymmetry the whole unit rule rests on.

    A package-keyed census scored `forze_kms` green on `forze_kms.aws` while `gcp` and `yc`
    had no code anywhere. The root really is demonstrated by a submodule's import; the
    siblings are not.
    """
    _write(corpus_root, "```python\nfrom forze_mock.server import MockApp\n```")
    manifest = _loaded(
        tmp_path,
        units='"forze" = { doctrine = "D2" }\n"forze_mock" = { doctrine = "D2" }\n'
        '"forze_mock.server" = { doctrine = "D2" }\n',
    )

    violations = check_census(load_corpus(corpus_root), manifest).violations

    assert any("forze:" in violation for violation in violations), "an unrelated root is not proven"
    assert not any("forze_mock:" in violation for violation in violations)
    assert not any("forze_mock.server:" in violation for violation in violations)


def test_a_submodule_imported_by_name_proves_that_submodule(
    corpus_root: Path, tmp_path: Path
) -> None:
    """`from forze_mock import server` imports a submodule, and the import gate resolves it.

    Recording only the left-hand module left the census calling a unit unproven while the
    gate beside it reported the same line resolved — two checks disagreeing about one
    import, with the census the stricter of the two.
    """
    _write(corpus_root, "```python\nfrom forze_mock import server\n```")
    manifest = _loaded(tmp_path)

    result = check_census(load_corpus(corpus_root), manifest)

    assert not any("forze_mock.server" in violation for violation in result.violations)


def test_a_project_with_no_extras_is_a_valid_project(tmp_path: Path) -> None:
    """A missing `[project.optional-dependencies]` is an empty set, not a read failure.

    Raising here reported the *package* list as unreadable while the package list was
    fine. It is not a hole either: with no extras the manifest's own rows become
    "`kms-aws` is not an extra in pyproject.toml", which fails loudly.
    """
    pyproject = tmp_path / "no-extras.toml"
    pyproject.write_text(
        '[project]\nname = "x"\n[tool.hatch.build.targets.wheel]\npackages = ["src/forze"]\n',
        encoding="utf-8",
    )

    assert load_extras(pyproject) == frozenset()
    assert load_shipped_packages(pyproject) == frozenset({"forze"})


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("extras = []\n", "`extras` must be a table"),
        ("units = []\n", "`units` must be a table"),
        ("extra-units = []\n", "`extra-units` must be a table"),
        ('[extras]\nsubdivides = []\n', "`subdivides` must be a table"),
        ('[extras]\nwhole-package = "nope"\n', "`whole-package` must be a table"),
        ('[extras.whole-package]\nnames = "postgres"\n', "must be a list of strings"),
        ('[extras.subdivides]\n"kms-aws" = 3\n', "maps to int, not a string"),
    ],
    ids=[
        "extras-array",
        "units-array",
        "extra-units-array",
        "subdivides-array",
        "whole-package-string",
        "names-string",
        "mapping-value-not-string",
    ],
)
def test_a_section_of_the_wrong_shape_is_named_not_crashed_on(
    tmp_path: Path, body: str, expected: str
) -> None:
    """Valid TOML can put a list where a table belongs.

    Two failure modes, and the quieter one is worse. `units = []` reached `.items()` and
    raised an `AttributeError` out of a build step — a traceback where the reader needed a
    sentence naming the section. `subdivides = []` did not raise at all: it read as an empty
    mapping and silently dropped every sub-unit from the denominator.
    """
    path = tmp_path / "shape.toml"
    path.write_text(body, encoding="utf-8")

    manifest = load_manifest(path, frozenset(), frozenset())

    assert any(expected in violation for violation in manifest.violations), manifest.violations


def test_a_census_with_nothing_to_prove_is_refused(corpus_root: Path, tmp_path: Path) -> None:
    """A manifest where every unit is out of scope reads "0/0 proven" — full coverage.

    The same zero-denominator pass the syntax and import gates already refuse. It cannot be
    reached by deleting rows, because totality catches that; it is reached by triaging
    everything into D3, which is a decision that should be loud rather than green.
    """
    manifest = _loaded(
        tmp_path,
        units='"forze" = { doctrine = "D3", rationale = "no" }\n'
        '"forze_mock" = { doctrine = "D3", rationale = "no" }\n'
        '"forze_mock.server" = { doctrine = "D3", rationale = "no" }\n',
    )

    result = check_census(load_corpus(corpus_root), manifest)

    assert manifest.violations == [], "the manifest itself is valid — that is the point"
    assert not result.ok
    assert any("proves nothing" in violation for violation in result.violations)


def test_census_is_keyed_on_wheel_packages() -> None:
    """Extras and packages are not interchangeable; imports are what the corpus claims."""
    assert "forze_postgres" in load_shipped_packages(_REPO / "pyproject.toml")


def test_the_shipped_manifest_is_total_over_the_real_inputs() -> None:
    """The §10-style bundle test: the manifest committed here matches this repository."""
    pyproject = _REPO / "pyproject.toml"
    manifest = load_manifest(
        default_manifest_path(), load_shipped_packages(pyproject), load_extras(pyproject)
    )

    assert manifest.violations == []
    assert manifest.proven, "a manifest with nothing to prove would pass vacuously"


# ----------------------- #
# Published-link liveness (§3.4) — pacing, retry budget, and what is not retried.


def _stub(*statuses: int | None):
    """A fetcher returning the given statuses in order, then repeating the last."""
    calls: list[str] = []

    def fetch(url: str, _timeout: float) -> tuple[int | None, str]:
        status = statuses[min(len(calls), len(statuses) - 1)]
        calls.append(url)

        return status, "stubbed"

    fetch.calls = calls  # type: ignore[attr-defined]

    return fetch


_NO_WAIT = LinkPolicy(pacing_seconds=0.0, backoff_seconds=0.0, attempts=3)


def test_live_url_passes() -> None:
    outcomes = check_liveness(("https://example.test/a",), _NO_WAIT, _stub(200))

    assert [outcome.ok for outcome in outcomes] == [True]


def test_transient_failure_is_retried_and_recovers() -> None:
    """The whole rationale for retries: a CDN hiccup must not read as a dead page."""
    fetch = _stub(502, 200)
    outcomes = check_liveness(("https://example.test/a",), _NO_WAIT, fetch)

    assert outcomes[0].ok
    assert outcomes[0].attempts == 2


def test_persistent_transient_failure_exhausts_the_budget_and_fails() -> None:
    fetch = _stub(503)
    outcomes = check_liveness(("https://example.test/a",), _NO_WAIT, fetch)

    assert not outcomes[0].ok
    assert outcomes[0].attempts == _NO_WAIT.attempts


def test_a_dead_page_is_not_retried() -> None:
    """404 is the answer. Spending the budget on it delays the report without changing it."""
    fetch = _stub(404)
    outcomes = check_liveness(("https://example.test/a",), _NO_WAIT, fetch)

    assert not outcomes[0].ok
    assert outcomes[0].attempts == 1


def test_transport_failure_without_a_status_is_retried() -> None:
    fetch = _stub(None, None, 200)
    outcomes = check_liveness(("https://example.test/a",), _NO_WAIT, fetch)

    assert outcomes[0].ok
    assert outcomes[0].attempts == 3


def test_the_sweep_bounds_its_own_duration_and_reports_what_it_skipped() -> None:
    """Worst case is 64 URLs x (3 x 15s + 6s backoff) — about 55 minutes.

    A sweep that only prints after finishing gets killed by the job limit having reported
    nothing, so the run costs an hour and yields no information. The budget makes the
    duration a property of the policy, and every URL it did not reach is named.
    """
    urls = ("https://example.test/a", "https://example.test/b", "https://example.test/c")
    spent = LinkPolicy(pacing_seconds=0.0, backoff_seconds=0.0, attempts=1, budget_seconds=0.0)

    outcomes = check_liveness(urls, spent, _stub(200))

    assert [outcome.checked for outcome in outcomes] == [False, False, False]
    assert [outcome.url for outcome in outcomes] == list(urls), "none may be dropped"
    assert all("budget exhausted" in outcome.detail for outcome in outcomes)


def test_an_unchecked_url_is_neither_dead_nor_live(corpus_root: Path) -> None:
    """Folding it into either count claims something the sweep never observed."""
    _write(corpus_root, "See https://morzecrew.github.io/forze/latest/in-depth/dst/ too.")
    outcome = LinkOutcome(url="https://example.test/a", status=None, detail="x", attempts=0)

    assert not outcome.ok
    assert not outcome.checked


def test_a_non_http_url_is_never_fetched() -> None:
    """`urlopen` honours `file:` and any registered scheme.

    Today's only caller matches against a pattern anchored to `https://<host>/`, which
    makes this unreachable — the guarantee belongs with the function that needs it rather
    than with whoever calls it next.
    """
    status, detail = _fetch("file:///etc/passwd", 1.0)

    assert status is None
    assert "refusing to fetch" in detail


def test_published_urls_are_collected_from_prose_not_only_links(corpus_root: Path) -> None:
    """A bare URL in prose is as much a claim about a live page as a Markdown link."""
    _write(corpus_root, "Read https://morzecrew.github.io/forze/latest/in-depth/dst/ first.")

    urls = collect_published_urls(load_corpus(corpus_root))

    assert "https://morzecrew.github.io/forze/latest/in-depth/dst/" in urls


# ----------------------- #
# The command line — the policy that decides what an exit code means.


def _run(root: Path, *args: str) -> int:
    return main(["--corpus", str(root), "--pyproject", str(_REPO / "pyproject.toml"), *args])


def test_cli_passes_on_the_real_corpus() -> None:
    """The gate's own claim, made by the entry point CI and `just` actually invoke."""
    assert _run(_REPO / "skills") == 0


def test_cli_reports_a_missing_corpus_distinctly(tmp_path: Path) -> None:
    """Exit 2, not 1: "the corpus is not there" is not "the corpus is broken"."""
    assert _run(tmp_path / "absent") == 2


@pytest.mark.parametrize(
    ("name", "content"),
    [
        ("absent.toml", None),
        ("malformed.toml", "this is not = valid toml [[[\n"),
        ("unrelated.toml", '[project]\nname = "something-else"\n'),
    ],
    ids=["missing", "malformed", "no-wheel-table"],
)
def test_cli_reports_an_unreadable_pyproject_distinctly(
    corpus_root: Path, tmp_path: Path, name: str, content: str | None
) -> None:
    """Both paths this command takes are arguments, so both fail the same way.

    One raising a traceback while the other returns a code makes the caller work out
    which argument it got wrong before it can read the failure at all.
    """
    pyproject = tmp_path / name

    if content is not None:
        pyproject.write_text(content, encoding="utf-8")

    assert main(["--corpus", str(corpus_root), "--pyproject", str(pyproject)]) == 2


def test_cli_fails_on_a_broken_corpus(corpus_root: Path) -> None:
    _write(corpus_root, "```python\nfrom forze.base.exceptions import NoSuchThing\n```")

    assert _run(corpus_root) == 1


def test_cli_fails_when_a_module_could_only_be_skipped(
    corpus_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A skip is not a pass. Reporting one in the output of a green run persuades nobody,
    because nobody reads the output of a green run.

    Driven by a stub package that is genuinely installed and genuinely will not import —
    the shape a missing extra has — rather than by skipping when the local environment
    happens to be complete, which would leave this policy unproven exactly where it is
    hardest to reach.
    """
    (tmp_path / "forze_stub").mkdir()
    (tmp_path / "forze_stub" / "__init__.py").write_text(
        "import a_third_party_package_that_is_not_installed\n", encoding="utf-8"
    )
    (tmp_path / "stub-pyproject.toml").write_text(
        "[project.optional-dependencies]\n\n"
        '[tool.hatch.build.targets.wheel]\npackages = ["src/forze", "src/forze_stub"]\n',
        encoding="utf-8",
    )
    # A manifest matching that stub world, so this test stays about a skipped import. The
    # committed manifest describes the real package list and would fail the census here for
    # reasons that have nothing to do with what is under test.
    manifest = _manifest(
        tmp_path,
        units='"forze" = { doctrine = "D1" }\n"forze_stub" = { doctrine = "D1" }\n',
        subdivides="",
        whole="[]",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    _write(
        corpus_root,
        "```python\nimport forze_stub\nfrom forze.base.exceptions import CoreException\n```",
    )

    stub = [
        "--corpus",
        str(corpus_root),
        "--pyproject",
        str(tmp_path / "stub-pyproject.toml"),
        "--manifest",
        str(manifest),
    ]

    assert main(stub) == 1
    assert main([*stub, "--allow-skips"]) == 0


def test_cli_reports_dead_links_and_fails(corpus_root: Path) -> None:
    _write(corpus_root, "See https://morzecrew.github.io/forze/latest/in-depth/dst/ too.")

    assert _run_liveness(load_corpus(corpus_root), lambda _url, _timeout: (404, "gone")) == 1
    assert _run_liveness(load_corpus(corpus_root), lambda _url, _timeout: (200, "ok")) == 0


def test_cli_refuses_a_liveness_sweep_with_nothing_to_sweep(tmp_path: Path) -> None:
    """The other vacuous pass: zero URLs checked is not zero URLs dead."""
    root = tmp_path / "skills"
    (root / SKILL_DIR / "references").mkdir(parents=True)
    (root / SKILL_DIR / "SKILL.md").write_text(
        _skill(note="## Reference\n\n> latest\n"), encoding="utf-8"
    )
    (root / SKILL_DIR / "references" / "demo.md").write_text(
        "# Demo\n\n## Reference\n\n- [x](../SKILL.md)\n", encoding="utf-8"
    )

    assert _run(root, "--links") == 1


# ----------------------- #
# The consolidated shape (RFC 0041) — the post-conditions the split has to hold.


def test_a_nested_skill_file_is_still_loaded(corpus_root: Path) -> None:
    """Excluded by identity, not by filename.

    A file called `SKILL.md` under `references/` matches no skill glob and, if the loader
    skipped it by name, no reference either — unchecked content that an installer copying
    recursively still ships.
    """
    nested = corpus_root / SKILL_DIR / "references" / "SKILL.md"
    nested.write_text("# Sneaky\n\n```python\ndef broken(:\n```\n", encoding="utf-8")

    corpus = load_corpus(corpus_root)

    assert nested in {doc.path for doc in corpus.documents}
    assert any("does not parse" in v for v in check_syntax(corpus).violations)


def test_more_than_one_published_skill_is_reported(corpus_root: Path) -> None:
    """§7's post-condition: an installer cannot prune what it is not overwriting."""
    (corpus_root / "forze-leftover").mkdir()
    (corpus_root / "forze-leftover" / "SKILL.md").write_text(
        _skill(name="forze-leftover"), encoding="utf-8"
    )

    violations = _violations(corpus_root, "structure")

    assert any("expected exactly one published skill" in v for v in violations)


def test_an_index_with_no_references_is_reported(corpus_root: Path) -> None:
    (corpus_root / SKILL_DIR / "references" / "demo.md").unlink()
    _write_index(corpus_root, routes=())

    violations = _violations(corpus_root, "structure")

    assert any("none were found" in v for v in violations)


# ----------------------- #
# RFC 0041 §10 — the real corpus, not a synthetic one.

_REFERENCES = _REPO / "skills" / "forze-skills" / "references"
_INDEX = _REPO / "skills" / "forze-skills" / "SKILL.md"

_BUNDLES = [
    ("architecture", "spec-naming-and-routes", "deps-resolution", "runtime-lifecycle"),
    (
        "aggregate-models",
        "document-spec",
        "aggregate-kit",
        "spec-to-backend-config",
        "testing-with-mock",
    ),
    ("execution-context", "handlers", "query-dsl"),
    ("fastapi-setup", "fastapi-generated-routes", "fastapi-identity"),
    ("field-encryption", "kms-backends", "spec-to-backend-config"),
    ("dst-simulation", "dst-invariants", "testing-with-mock"),
]


@pytest.mark.parametrize("bundle", _BUNDLES, ids=[b[0] for b in _BUNDLES])
def test_every_routing_bundle_is_reachable(bundle: tuple[str, ...]) -> None:
    """A bundle naming a reference that does not exist is a dead end for a cold reader.

    This is the mechanical floor under §10's behavioural criterion, not the criterion
    itself: it proves the row *can* be followed, not that an agent follows all of it.
    """
    index = _INDEX.read_text(encoding="utf-8")

    for stem in bundle:
        assert (_REFERENCES / f"{stem}.md").is_file(), f"{stem} is routed to but absent"
        assert f"references/{stem}.md" in index, f"{stem} is bundled but not in the index"


def test_the_index_states_the_read_more_than_one_norm() -> None:
    """§5's compensation for losing the harness's own description matcher."""
    index = _INDEX.read_text(encoding="utf-8").lower()

    assert "reading one" in index or "read the bundle" in index


def test_no_reference_exceeds_the_ceiling() -> None:
    """Over 250 lines means it is two jobs — the defect the split existed to fix."""
    oversized = {
        path.stem: len(path.read_text(encoding="utf-8").split("\n"))
        for path in _REFERENCES.glob("*.md")
        if len(path.read_text(encoding="utf-8").split("\n")) > 250
    }

    assert oversized == {}
