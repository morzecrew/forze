"""The dep-key attribution bar, seen red (.github/scripts/docs_floors.py).

The symbol bar next to it only asks whether a contract symbol is *mentioned* anywhere in
`pages/docs`, which an index of 75 dep keys satisfies while attributing any of them to the
wrong capability — a table that confidently names the wrong key is worse than a table with
no key column at all, because a reader trusts it.

So the bar under test compares each row's keys against the accessors the row itself shows,
using the accessor's own source as the authority. The tests inject the swap, the omission
and the shape it cannot see, over a synthetic page in `tmp_path`: running the real page
would pass against a checker that returns nothing, which is the failure mode that matters
here (the published page is green, so a dead check and a correct page look identical).
The accessor side is deliberately *not* synthetic — its whole value is the link to the
live `ExecutionContext`.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

pytestmark = pytest.mark.unit

# ----------------------- #

_REPO = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO / ".github" / "scripts" / "docs_floors.py"


def _load_checker() -> ModuleType:
    spec = importlib.util.spec_from_file_location("docs_floors", _SCRIPT)

    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load checker script at {_SCRIPT}")

    module = importlib.util.module_from_spec(spec)
    # dataclass processing resolves the defining module via sys.modules, so the script
    # must be registered before exec, like a normal import would.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    return module


checker = _load_checker()

_HEADER = (
    "| Capability | Spec | Resolve via | Dep key |\n|------------|------|-------------|---------|\n"
)


def _index(root: Path, *rows: str) -> object:
    """A policy pointing at a synthetic index page holding *rows*."""

    page = root / "reference" / "contracts.md"
    page.parent.mkdir(parents=True, exist_ok=True)
    page.write_text(_HEADER + "".join(f"{row}\n" for row in rows), encoding="utf-8")

    return checker.Policy(
        docs_root=root,
        nav_config=root / "unused.toml",
        dep_key_index=Path("reference/contracts.md"),
    )


# ----------------------- #


class TestTheAccessorMapping:
    def test_it_is_read_from_the_accessors_own_source(self) -> None:
        # The authority is the code, not a table kept beside it: `ctx.cache` resolves the
        # cache key and nothing else claims it.
        owners = checker.accessor_key_owners()

        assert owners["CacheDepKey"] == frozenset({"cache"})
        assert "counter" in owners["CounterDepKey"]

    def test_a_key_no_accessor_resolves_is_absent_rather_than_guessed(self) -> None:
        # The durable family and the queue keys have no `ctx` accessor. Absent means "not
        # checkable", which is the honest answer; inventing an owner would put a guess in
        # a gate and fail a correct page.
        owners = checker.accessor_key_owners()

        assert "DurableRunStoreDepKey" not in owners
        assert "QueueCommandDepKey" not in owners


class TestTheAttributionBar:
    def test_a_row_carrying_another_capabilitys_key_is_reported(self, tmp_path: Path) -> None:
        policy = _index(
            tmp_path,
            "| Cache | `CacheSpec` | `ctx.cache(spec)` | `CounterDepKey` |",
        )
        violations = checker.check_dep_key_attribution(policy)

        assert len(violations) == 1
        assert "row 'Cache' names CounterDepKey" in violations[0]
        assert "ctx.counter resolves" in violations[0]

    def test_a_swap_between_two_rows_is_reported_on_both(self, tmp_path: Path) -> None:
        # The mutation the bar exists for: two rows keep their accessors and exchange
        # their keys, so every symbol is still mentioned and the symbol bar stays green.
        policy = _index(
            tmp_path,
            "| Cache | `CacheSpec` | `ctx.cache(spec)` | `CounterDepKey` |",
            "| Counter | `CounterSpec` | `ctx.counter(spec)` | `CacheDepKey` |",
        )
        violations = checker.check_dep_key_attribution(policy)

        assert len(violations) == 2
        assert {"Cache", "Counter"} == {
            violation.split("row '")[1].split("'")[0] for violation in violations
        }

    def test_a_correctly_attributed_row_passes(self, tmp_path: Path) -> None:
        policy = _index(
            tmp_path,
            "| Cache | `CacheSpec` | `ctx.cache(spec)` | `CacheDepKey` |",
            "| Counter | `CounterSpec` | `ctx.counter(spec)` | `CounterDepKey` "
            "(+ `CounterAdminDepKey` for reset / drop) |",
        )

        assert checker.check_dep_key_attribution(policy) == []

    def test_the_accessor_may_sit_in_any_cell_of_the_row(self, tmp_path: Path) -> None:
        # Some rows name the accessor inside the key cell's parenthetical ("`ctx.stream`
        # shortcuts for the commit sub-model"), so the row is searched whole rather than
        # column by column.
        policy = _index(
            tmp_path,
            "| Cache | `CacheSpec` | by dep key | `CacheDepKey` (via `ctx.cache(spec)`) |",
        )

        assert checker.check_dep_key_attribution(policy) == []

    def test_a_key_with_no_accessor_is_not_second_guessed(self, tmp_path: Path) -> None:
        # `DurableRunStoreDepKey` is resolved by dep key alone, so the bar has nothing to
        # compare and must stay silent rather than demand an accessor that does not exist.
        policy = _index(
            tmp_path,
            "| Run stores | — | by dep key | `DurableRunStoreDepKey` |",
        )

        assert checker.check_dep_key_attribution(policy) == []

    def test_a_row_showing_no_accessor_still_owes_one(self, tmp_path: Path) -> None:
        # "by dep key" is how the durable and queue rows are written, and it must not
        # double as a way to silence the bar: the cache key *is* accessor-resolved, so a
        # row claiming otherwise is wrong even though it names no accessor to contradict.
        policy = _index(
            tmp_path,
            "| Cache | `CacheSpec` | by dep key | `CacheDepKey` |",
        )
        violations = checker.check_dep_key_attribution(policy)

        assert len(violations) == 1
        assert "ctx.cache resolves" in violations[0]

    def test_a_key_named_outside_the_key_cell_is_not_an_attribution(self, tmp_path: Path) -> None:
        # Only the last column attributes. A key named in prose or in the accessor cell is
        # a cross-reference, and reading the whole row for keys would make a correct
        # sentence about a neighbouring plane fail the build.
        policy = _index(
            tmp_path,
            "| Cache | `CacheSpec` | `ctx.cache(spec)` — not `CounterDepKey` | `CacheDepKey` |",
        )

        assert checker.check_dep_key_attribution(policy) == []

    def test_prose_outside_the_tables_is_not_parsed_as_a_row(self, tmp_path: Path) -> None:
        # The page's closing paragraphs explain the column and may well name a key while
        # doing so. Only table rows attribute, so a sentence must never be read as one.
        policy = _index(
            tmp_path,
            "| Cache | `CacheSpec` | `ctx.cache(spec)` | `CacheDepKey` |",
        )
        page = tmp_path / "reference" / "contracts.md"
        page.write_text(
            page.read_text(encoding="utf-8")
            + "\nWiring a counter yourself means binding `CounterDepKey` by hand.\n",
            encoding="utf-8",
        )

        assert checker.check_dep_key_attribution(policy) == []

    def test_rows_without_a_key_cell_are_skipped(self, tmp_path: Path) -> None:
        policy = _index(
            tmp_path,
            "| Realtime egress | catalog | `build_realtime_publisher(ctx, …)` | — |",
        )

        assert checker.check_dep_key_attribution(policy) == []


class TestTheBarIsActuallyWired:
    """A bar that exists and never runs is the failure this repository keeps finding.

    Two ways it could happen here: the check is written but left out of `main`'s violation
    sum, or the config line that arms it is dropped from `pyproject.toml`. Neither is
    visible in the checker's own unit tests, so both are pinned.
    """

    def test_the_repository_arms_the_bar(self) -> None:
        policy = checker.load_policy(_REPO / "pyproject.toml")

        assert policy.dep_key_index == Path("reference/contracts.md")

    def test_main_fails_on_a_swap(self, tmp_path: Path) -> None:
        # End-to-end through the entry point the justfile calls, over a synthetic corpus:
        # this is what fails if the check is dropped from `main`'s violation sum.
        docs = tmp_path / "docs"
        _index(docs, "| Cache | `CacheSpec` | `ctx.cache(spec)` | `CounterDepKey` |")
        every_symbol = ", ".join(f"`{name}`" for name in checker.discover_symbols())
        (docs / "everything.md").write_text(every_symbol, encoding="utf-8")
        (tmp_path / "nav.toml").write_text(
            'nav = ["reference/contracts.md", "everything.md"]\n', encoding="utf-8"
        )
        (tmp_path / "pyproject.toml").write_text(
            "[tool.docs_floors]\n"
            f'docs_root = "{docs}"\n'
            f'nav_config = "{tmp_path / "nav.toml"}"\n'
            'dep_key_index = "reference/contracts.md"\n',
            encoding="utf-8",
        )

        assert checker.main(["--pyproject", str(tmp_path / "pyproject.toml")]) == 1


class TestTheBarsOwnLimits:
    def test_no_index_configured_skips_the_bar(self, tmp_path: Path) -> None:
        policy = checker.Policy(docs_root=tmp_path, nav_config=tmp_path / "unused.toml")

        assert checker.check_dep_key_attribution(policy) == []

    def test_a_configured_index_that_does_not_exist_fails_rather_than_passes(
        self, tmp_path: Path
    ) -> None:
        # A renamed page must not retire the bar silently — the same reason the link check
        # resolves generated targets back to their sources instead of skipping them.
        policy = checker.Policy(
            docs_root=tmp_path,
            nav_config=tmp_path / "unused.toml",
            dep_key_index=Path("reference/gone.md"),
        )
        violations = checker.check_dep_key_attribution(policy)

        assert len(violations) == 1
        assert "not found" in violations[0]
