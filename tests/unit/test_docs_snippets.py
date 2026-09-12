"""Every docs-snippet bar, seen red.

Each bar is driven by *injecting* the regression it exists to catch — a block that stopped
parsing, an import that no longer resolves, a call that no longer matches its signature —
and asserting that this specific bar reports it. A test that only ran the real
`pages/docs` would pass against a checker returning "ok" unconditionally, and on this gate
that failure mode is especially easy to miss: the published corpus is green, so a broken
checker and a clean corpus look identical.

The corpus under test is synthetic and built in `tmp_path`, so a test says what it is
about rather than depending on which page happens to hold which example today. Import and
signature resolution are the exception and run against the *really installed* packages:
their whole value is the link to the live API, and a mocked importer would prove only that
the mock agrees with itself.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tools.docs_snippets import (
    check_call_shapes,
    check_imports,
    check_syntax,
    load_blocks,
    main,
)

pytestmark = pytest.mark.unit


def _page(root: Path, body: str, name: str = "page.md") -> Path:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")

    return path


def _blocks(root: Path) -> tuple:
    return load_blocks(root)[0]


def _fence(code: str, markers: str = "") -> str:
    info = f"python {markers}".strip()

    return f"Prose.\n\n```{info}\n{code}\n```\n"


# ----------------------- #
# Bar 1 — parse


class TestSyntax:
    def test_a_block_that_no_longer_parses_is_reported_with_its_line(self, tmp_path: Path) -> None:
        _page(tmp_path, _fence("def broken(:\n    pass"))
        result = check_syntax(_blocks(tmp_path), ())

        assert not result.ok
        assert "does not parse" in result.violations[0]
        assert "page.md:3" in result.violations[0]

    def test_a_clean_block_passes_and_the_summary_counts_it(self, tmp_path: Path) -> None:
        _page(tmp_path, _fence("x = 1"))
        result = check_syntax(_blocks(tmp_path), ())

        assert result.ok
        assert result.summary == "1/1 block(s) parsed, 0 marked fragment"

    def test_a_fragment_marker_excuses_a_block_that_cannot_parse(self, tmp_path: Path) -> None:
        _page(tmp_path, _fence("build_runtime(deps=deps, ...)", markers="fragment"))
        result = check_syntax(_blocks(tmp_path), ())

        assert result.ok
        assert "1 marked fragment" in result.summary

    def test_a_fragment_marker_on_a_block_that_parses_is_itself_a_failure(
        self, tmp_path: Path
    ) -> None:
        """The marker is load-bearing in both directions, or it becomes an opt-out.

        Lifted from the skills corpus's rule rather than re-decided: two gates in one
        repository disagreeing about how an unverifiable snippet is declared would be worse
        than either answer.
        """

        _page(tmp_path, _fence("x = 1", markers="fragment"))
        result = check_syntax(_blocks(tmp_path), ())

        assert not result.ok
        assert "parses fine — drop the marker" in result.violations[0]

    def test_an_unknown_marker_is_refused_naming_what_is_defined(self, tmp_path: Path) -> None:
        _page(tmp_path, _fence("x = 1", markers="skipme"))
        result = check_syntax(_blocks(tmp_path), ())

        assert not result.ok
        assert "unknown fence marker(s) ['skipme']" in result.violations[0]

    def test_an_empty_corpus_is_a_checker_failure_not_a_pass(self, tmp_path: Path) -> None:
        """What this gate looks like once the extractor stops finding anything.

        A changed fence convention or a moved docs root produces "0/0 blocks parsed, ok",
        which is indistinguishable from a corpus with no code in it — so the refusal lives
        at the seam that knows the denominator.
        """

        _page(tmp_path, "Prose only, no code.\n")
        result = check_syntax(_blocks(tmp_path), ())

        assert not result.ok
        assert "not one inline python block" in result.violations[0]

    def test_an_unclosed_fence_is_reported(self, tmp_path: Path) -> None:
        page = _page(tmp_path, "Prose.\n\n```python\nx = 1\n")
        inline, _ = load_blocks(tmp_path)
        unclosed = tuple(block for block in inline if not block.closed)

        assert unclosed, "the loader should see the fence is never closed"

        result = check_syntax(inline, unclosed)

        assert not result.ok
        assert "never closed" in result.violations[0]
        assert page.name in result.violations[0]


# ----------------------- #
# Bar 2 — imports


class TestImports:
    def test_a_symbol_that_no_longer_exists_is_named(self, tmp_path: Path) -> None:
        _page(tmp_path, _fence("from forze_identity import ThisWasRenamed"))
        result = check_imports(_blocks(tmp_path))

        assert not result.ok
        assert "has no 'ThisWasRenamed'" in result.violations[0]

    def test_a_module_that_no_longer_exists_is_named(self, tmp_path: Path) -> None:
        _page(tmp_path, _fence("from forze_identity.gone import thing"))
        result = check_imports(_blocks(tmp_path))

        assert not result.ok
        assert "fails" in result.violations[0]

    def test_a_live_symbol_resolves(self, tmp_path: Path) -> None:
        _page(tmp_path, _fence("from forze_identity import spec_contributions"))
        result = check_imports(_blocks(tmp_path))

        assert result.ok
        assert result.summary == "1/1 forze import(s) resolve"

    def test_a_non_forze_import_is_not_this_gate_s_business(self, tmp_path: Path) -> None:
        _page(tmp_path, _fence("from collections import OrderedDict"))
        result = check_imports(_blocks(tmp_path))

        assert result.ok
        assert result.summary == "0/0 forze import(s) resolve"

    def test_a_fragment_is_not_import_checked(self, tmp_path: Path) -> None:
        """A marked block does not parse, so there is nothing to read imports out of."""

        _page(
            tmp_path,
            _fence("from forze_identity import ThisWasRenamed\nf(a=1, ...)", markers="fragment"),
        )

        assert check_imports(_blocks(tmp_path)).ok


# ----------------------- #
# Bar 3 — call shapes


class TestCallShapes:
    def test_a_call_missing_a_required_keyword_is_reported(self, tmp_path: Path) -> None:
        """The regression that motivated the gate, in the shape it actually had.

        `build_realtime_mailbox(ctx)` shipped on two pages after the function gained a
        required keyword-only `retention`. The symbol still exists, so bars 1 and 2 pass it
        — this is the only bar that reaches it.
        """

        _page(
            tmp_path,
            _fence(
                "from forze_kits.integrations.realtime import build_realtime_mailbox\n"
                "mailbox = build_realtime_mailbox(ctx)"
            ),
        )
        result = check_call_shapes(_blocks(tmp_path))

        assert not result.ok
        assert "does not match build_realtime_mailbox" in result.violations[0]
        assert "retention" in result.violations[0]

    def test_a_keyword_that_no_longer_exists_is_reported(self, tmp_path: Path) -> None:
        _page(
            tmp_path,
            _fence(
                "from forze_identity import spec_contributions\n"
                "specs = spec_contributions(plane='authn')"
            ),
        )
        result = check_call_shapes(_blocks(tmp_path))

        assert not result.ok
        assert "unexpected keyword argument" in result.violations[0]

    def test_a_correct_call_passes(self, tmp_path: Path) -> None:
        _page(
            tmp_path,
            _fence(
                "from forze_identity import spec_contributions\n"
                "specs = spec_contributions(planes=['authn'])"
            ),
        )
        result = check_call_shapes(_blocks(tmp_path))

        assert result.ok
        assert result.summary == "1/1 call(s) match their signature (0 not on an imported symbol)"

    def test_an_elided_call_states_no_shape_and_is_skipped(self, tmp_path: Path) -> None:
        """`f(..., x=1)` parses but declares nothing to bind — guessing would invent a failure."""

        _page(
            tmp_path,
            _fence("from forze_identity import spec_contributions\nspec_contributions(...)"),
        )
        result = check_call_shapes(_blocks(tmp_path))

        assert result.ok
        assert result.summary.startswith("0/0 call(s) match their signature")

    def test_a_spread_call_is_skipped(self, tmp_path: Path) -> None:
        _page(
            tmp_path,
            _fence("from forze_identity import spec_contributions\nspec_contributions(**options)"),
        )

        assert check_call_shapes(_blocks(tmp_path)).summary.startswith(
            "0/0 call(s) match their signature"
        )

    def test_a_classmethod_on_an_imported_class_is_checked(self, tmp_path: Path) -> None:
        """`DepsRegistry.from_modules(...)` is a third of what the docs actually call.

        The first cut only resolved bare names, which left every classmethod and every
        imported namespace object unchecked — measured at 41% of the calls in the corpus.
        """

        _page(
            tmp_path,
            _fence(
                "from forze.application.execution import DepsRegistry\n"
                "registry = DepsRegistry.from_modules(module, nonsense=1)"
            ),
        )
        result = check_call_shapes(_blocks(tmp_path))

        assert not result.ok
        assert "does not match DepsRegistry.from_modules" in result.violations[0]

    def test_a_method_on_an_imported_namespace_object_is_checked(self, tmp_path: Path) -> None:
        """`exc` is an object, not a callable, and `exc.domain(...)` is still resolvable."""

        _page(
            tmp_path,
            _fence("from forze.base.exceptions import exc\nraise exc.domain(bad_kwarg=1)"),
        )
        result = check_call_shapes(_blocks(tmp_path))

        assert not result.ok
        assert "exc.domain" in result.violations[0]

    def test_a_call_on_a_local_instance_is_skipped_and_counted(self, tmp_path: Path) -> None:
        """`runtime.scope()` needs to know what `runtime` was assigned — inference, not resolution.

        Counted in the summary rather than dropped silently, so the denominator cannot be
        read as "every call in the docs".
        """

        _page(
            tmp_path,
            _fence(
                "from forze.application.execution import build_runtime\n"
                "runtime = build_runtime(deps=deps)\n"
                "async with runtime.scope():\n    pass"
            ),
        )
        result = check_call_shapes(_blocks(tmp_path))

        assert result.ok
        assert "1/1 call(s) match their signature (1 not on an imported symbol)" in result.summary

    def test_a_call_to_something_the_block_did_not_import_is_not_checked(
        self, tmp_path: Path
    ) -> None:
        """The callee has to be a name this block bound to a live forze symbol.

        Otherwise the checker would be guessing which `create` a bare name refers to.
        """

        _page(tmp_path, _fence("spec_contributions(nonsense=1)"))

        assert check_call_shapes(_blocks(tmp_path)).ok


# ----------------------- #
# The loader and the entrypoint


class TestResolutionFailuresDoNotCrashTheBar:
    def test_a_broken_forze_import_leaves_the_call_bar_quiet(self, tmp_path: Path) -> None:
        """The import bar owns that finding; reporting it twice would double-count it.

        What matters here is that an unimportable module does not take the call bar down
        with it — the block still has other calls worth checking.
        """

        _page(
            tmp_path,
            _fence(
                "from forze_identity.gone import thing\n"
                "from forze_identity import spec_contributions\n"
                "thing()\n"
                "specs = spec_contributions(planes=['authn'])"
            ),
        )

        assert check_call_shapes(_blocks(tmp_path)).ok
        assert not check_imports(_blocks(tmp_path)).ok


class TestLoader:
    def test_an_included_block_is_left_to_the_example_tests(self, tmp_path: Path) -> None:
        """`--8<--` pulls its body from `examples/`, which a real test runs."""

        _page(tmp_path, _fence("--8<-- 'examples/quickstart/app.py'"))
        inline, included = load_blocks(tmp_path)

        assert not inline
        assert len(included) == 1

    def test_a_non_python_fence_is_ignored(self, tmp_path: Path) -> None:
        _page(tmp_path, 'Prose.\n\n```json\n{"not": "python"}\n```\n')

        assert not _blocks(tmp_path)

    def test_pages_are_found_recursively(self, tmp_path: Path) -> None:
        _page(tmp_path, _fence("x = 1"), name="a/b/deep.md")

        assert len(_blocks(tmp_path)) == 1


class TestEntrypoint:
    def test_it_exits_zero_on_a_clean_corpus(self, tmp_path: Path, capsys) -> None:
        _page(tmp_path, _fence("from forze_identity import spec_contributions"))

        assert main(["--root", str(tmp_path)]) == 0
        assert "passed" in capsys.readouterr().out

    def test_it_exits_one_and_names_the_bar_that_failed(self, tmp_path: Path, capsys) -> None:
        _page(tmp_path, _fence("from forze_identity import Gone"))

        assert main(["--root", str(tmp_path)]) == 1

        out = capsys.readouterr().out

        assert "FAIL imports" in out
        assert "failed" in out

    def test_the_real_docs_corpus_is_clean(self) -> None:
        """The gate over `pages/docs` itself, which is the one run in `just quality`.

        Last, and deliberately not the only case: this assertion is what a broken checker
        passes most easily.
        """

        assert main([]) == 0
