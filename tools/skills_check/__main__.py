"""Command-line entry point for the skills corpus gates."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .checks import (
    check_census,
    check_imports,
    check_structure,
    check_syntax,
    load_shipped_packages,
)
from .corpus import Corpus, load_corpus
from .links import DEFAULT_ATTEMPTS, LinkPolicy, check_liveness, collect_published_urls

# ----------------------- #

DEFAULT_CORPUS_ROOT = Path("skills")
DEFAULT_PYPROJECT = Path("pyproject.toml")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--corpus",
        default=DEFAULT_CORPUS_ROOT,
        type=Path,
        help=f"root of the published skills corpus (default: {DEFAULT_CORPUS_ROOT})",
    )
    parser.add_argument(
        "--pyproject",
        default=DEFAULT_PYPROJECT,
        type=Path,
        help=f"pyproject.toml holding the wheel package list (default: {DEFAULT_PYPROJECT})",
    )
    parser.add_argument(
        "--links",
        action="store_true",
        help="run published-link liveness instead of the offline gates (network, scheduled)",
    )
    parser.add_argument(
        "--allow-skips",
        action="store_true",
        help=(
            "pass even when a shipped package could not be imported. For a partial local "
            "install only — the gate's denominator shrinks silently without it"
        ),
    )
    args = parser.parse_args(argv)

    if not args.corpus.is_dir():
        print(f"skills-check: no corpus at {args.corpus}", file=sys.stderr)

        return 2

    corpus = load_corpus(args.corpus)

    if args.links:
        return _run_liveness(corpus)

    return _run_offline(corpus, args.pyproject, allow_skips=args.allow_skips)


# ----------------------- #


def _run_offline(corpus: Corpus, pyproject: Path, allow_skips: bool) -> int:
    shipped = load_shipped_packages(pyproject)
    results = [
        check_syntax(corpus),
        check_imports(corpus, shipped),
        check_structure(corpus),
        check_census(corpus, shipped),
    ]

    width = max(len(result.name) for result in results)

    for result in results:
        print(f"{result.name:<{width}}  {'ok ' if result.ok else 'FAIL'}  {result.summary}")

    failed = [result for result in results if not result.ok]
    skipped = [result for result in results if result.skips]

    for result in skipped:
        print(f"\n{result.name} — not checked here:")

        for skip in result.skips:
            print(f"  · {skip}")

    if failed:
        print(f"\nSkills check FAILED ({sum(len(r.violations) for r in failed)} violation(s)):")

        for result in failed:
            for violation in result.violations:
                print(f"  - {violation}")

        return 1

    # A skipped import is not a passing one. Left unremarked it turns an environment
    # missing an extra into a green run over a shrinking denominator, which is the
    # vacuous pass this whole file exists to make impossible.
    unresolved = next((result for result in results if result.name == "imports"), None)

    if unresolved is not None and unresolved.skips and not allow_skips:
        print(
            "\nSkills check FAILED: "
            f"{len(unresolved.skips)} module(s) could not be imported, so their symbols "
            "were never checked. Install the full extras set "
            "(`uv sync --all-groups --all-extras`), or pass --allow-skips to accept a "
            "partial run."
        )

        return 1

    print("\nSkills check passed.")

    return 0


def _run_liveness(corpus: Corpus) -> int:
    urls = collect_published_urls(corpus)

    if not urls:
        # An empty sweep is not a green one. A corpus with no published links means the
        # collector broke, not that every link is alive.
        print("Skills links FAILED: the corpus cites no published doc URLs at all.")

        return 1

    outcomes = check_liveness(urls, LinkPolicy())
    dead = [outcome for outcome in outcomes if not outcome.ok]

    for outcome in dead:
        status = outcome.status if outcome.status is not None else "no response"
        print(f"  - {outcome.url} -> {status} ({outcome.detail}), {outcome.attempts} attempt(s)")

    if dead:
        print(
            f"\nSkills links FAILED: {len(dead)}/{len(urls)} published URL(s) did not "
            f"return 200 after {DEFAULT_ATTEMPTS} attempt(s)."
        )

        return 1

    print(f"Skills links passed: {len(urls)}/{len(urls)} published URL(s) returned 200.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
