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
    load_shipped_packages,
)
from tools.skills_check.corpus import load_corpus
from tools.skills_check.links import (
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


def _skill(body: str = "", name: str = "forze-demo", reference: str = _REFERENCE) -> str:
    """Assemble a skill document.

    Built by concatenation rather than by dedenting an interpolated template: a
    multi-line ``body`` starting at column zero makes the common prefix empty, so the
    dedent silently does nothing and every line — the frontmatter delimiter included —
    stays indented. The resulting document then fails checks for reasons that have
    nothing to do with what the test is about.
    """
    return (
        "---\n"
        f"name: {name}\n"
        "description: >-\n"
        "  A demo skill. Use when testing the corpus gates.\n"
        "---\n"
        "\n"
        "# Demo\n"
        "\n"
        f"{body}\n"
        "\n"
        "## Anti-patterns\n"
        "\n"
        "- Doing the thing the wrong way.\n"
        "\n"
        f"{reference}"
    )


def _index(*names: str) -> str:
    rows = "\n".join(f"| **{name}** | A demo skill. |" for name in names)

    return f"# Skills\n\n| Name | Description |\n| ---- | ---- |\n{rows}\n"


_BASELINE_BODY = "```python\nfrom forze.base.exceptions import CoreException\n```"


@pytest.fixture
def corpus_root(tmp_path: Path) -> Path:
    """A minimal corpus that passes every gate, ready to be broken one way at a time.

    It carries a real python block on purpose: a corpus with none is itself a failure
    (see `test_a_corpus_with_no_python_blocks_is_a_failure`), so a baseline without one
    would be asserting the wrong green.
    """
    root = tmp_path / "skills"
    (root / "forze-demo").mkdir(parents=True)
    (root / "forze-demo" / "SKILL.md").write_text(_skill(_BASELINE_BODY), encoding="utf-8")
    (root / "README.md").write_text(_index("forze-demo"), encoding="utf-8")

    return root


def _write(root: Path, body: str = _BASELINE_BODY, **kwargs: str) -> None:
    (root / "forze-demo" / "SKILL.md").write_text(_skill(body, **kwargs), encoding="utf-8")


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
    (root / "README.md").write_text(_index(), encoding="utf-8")

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
    path = corpus_root / "forze-demo" / "SKILL.md"
    path.write_text(path.read_text(encoding="utf-8").replace("## Anti-patterns", "## Notes"))

    violations = _violations(corpus_root, "structure")

    assert len(violations) == 1
    assert "no `## Anti-patterns` section" in violations[0]


def test_frontmatter_name_must_match_the_directory(corpus_root: Path) -> None:
    _write(corpus_root, name="forze-renamed")

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
    _write(corpus_root, "See [the source](../../src/forze.py).")

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
    """`#### Anti-patterns` nests under whatever precedes it — it is not the section."""
    path = corpus_root / "forze-demo" / "SKILL.md"
    path.write_text(
        path.read_text(encoding="utf-8").replace("## Anti-patterns", "#### Anti-patterns"),
        encoding="utf-8",
    )

    violations = _violations(corpus_root, "structure")

    assert any("no `## Anti-patterns` section" in violation for violation in violations)


def test_reference_section_without_the_versioned_note_is_reported(corpus_root: Path) -> None:
    _write(
        corpus_root,
        reference=(
            "## Reference\n\n"
            "- [Wiring](https://morzecrew.github.io/forze/latest/writing-operation/wiring/)\n"
        ),
    )

    violations = _violations(corpus_root, "structure")

    assert any("versioned-docs note" in violation for violation in violations)


def test_index_parity_is_checked_in_both_directions(corpus_root: Path) -> None:
    (corpus_root / "README.md").write_text(_index("forze-ghost"), encoding="utf-8")

    violations = _violations(corpus_root, "structure")

    assert any("no such skill directory exists" in violation for violation in violations)
    assert any("is missing from the index table" in violation for violation in violations)


# ----------------------- #
# Census (§3.5) — report-only, on purpose.


def test_census_reports_gaps_without_failing(corpus_root: Path) -> None:
    _write(corpus_root, "```python\nfrom forze.base.exceptions import CoreException\n```")

    result = check_census(load_corpus(corpus_root), SHIPPED)

    assert result.ok, "coverage is RFC 0042's decision; this check only reports"
    assert any("absent" in skip for skip in result.skips)


def test_census_is_keyed_on_wheel_packages() -> None:
    """Extras and packages are not interchangeable; imports are what the corpus claims."""
    assert "forze_postgres" in load_shipped_packages(_REPO / "pyproject.toml")


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
        '[tool.hatch.build.targets.wheel]\npackages = ["src/forze", "src/forze_stub"]\n',
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    _write(corpus_root, "```python\nimport forze_stub\n```")

    stub = ["--corpus", str(corpus_root), "--pyproject", str(tmp_path / "stub-pyproject.toml")]

    assert main(stub) == 1
    assert main([*stub, "--allow-skips"]) == 0


def test_cli_reports_dead_links_and_fails(corpus_root: Path) -> None:
    _write(corpus_root, "See https://morzecrew.github.io/forze/latest/in-depth/dst/ too.")

    assert _run_liveness(load_corpus(corpus_root), lambda _url, _timeout: (404, "gone")) == 1
    assert _run_liveness(load_corpus(corpus_root), lambda _url, _timeout: (200, "ok")) == 0


def test_cli_refuses_a_liveness_sweep_with_nothing_to_sweep(tmp_path: Path) -> None:
    """The other vacuous pass: zero URLs checked is not zero URLs dead."""
    root = tmp_path / "skills"
    (root / "forze-demo").mkdir(parents=True)
    (root / "forze-demo" / "SKILL.md").write_text(
        _skill(_BASELINE_BODY, reference="## Reference\n\n> latest\n"), encoding="utf-8"
    )
    (root / "README.md").write_text(_index("forze-demo"), encoding="utf-8")

    assert _run(root, "--links") == 1
