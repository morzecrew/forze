"""Command-line entry point for the skills corpus gates."""

from __future__ import annotations

import argparse
import sys
import tomllib
from pathlib import Path

from .checks import (
    check_census,
    check_imports,
    check_structure,
    check_syntax,
    load_extras,
    load_shipped_packages,
)
from .corpus import Corpus, load_corpus
from .links import (
    DEFAULT_ATTEMPTS,
    Fetcher,
    LinkPolicy,
    check_liveness,
    collect_published_urls,
)
from .manifest import Manifest, default_manifest_path, load_manifest

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
        "--manifest",
        default=None,
        type=Path,
        help="coverage doctrine manifest (default: coverage.toml beside the checker)",
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

    try:
        shipped = load_shipped_packages(args.pyproject)
        extras = load_extras(args.pyproject)
    except (OSError, tomllib.TOMLDecodeError, KeyError, TypeError) as error:
        # Both paths this command takes are arguments, so both fail the same way. Letting
        # one raise a traceback while the other returns a code means the caller has to
        # know which argument it got wrong to know how to read the failure.
        print(
            f"skills-check: cannot read the wheel package list from {args.pyproject} "
            f"({type(error).__name__}: {error})",
            file=sys.stderr,
        )

        return 2

    manifest_path = args.manifest or default_manifest_path()

    if not manifest_path.is_file():
        # Same shape as --corpus and --pyproject: a path argument fails like one. A missing
        # manifest is not an empty manifest — reporting "every unit lacks a doctrine" would
        # be 38 violations describing one wrong path.
        print(f"skills-check: no coverage manifest at {manifest_path}", file=sys.stderr)

        return 2

    manifest = load_manifest(manifest_path, shipped, extras)

    return _run_offline(corpus, shipped, manifest, allow_skips=args.allow_skips)


# ----------------------- #


def _run_offline(
    corpus: Corpus, shipped: frozenset[str], manifest: Manifest, allow_skips: bool
) -> int:
    results = [
        check_syntax(corpus),
        check_imports(corpus, shipped),
        check_structure(corpus),
        check_census(corpus, manifest),
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


def _run_liveness(corpus: Corpus, fetcher: Fetcher | None = None) -> int:
    urls = collect_published_urls(corpus)

    if not urls:
        # An empty sweep is not a green one. A corpus with no published links means the
        # collector broke, not that every link is alive.
        print("Skills links FAILED: the corpus cites no published doc URLs at all.")

        return 1

    outcomes = check_liveness(urls, LinkPolicy(), fetcher)
    dead = [outcome for outcome in outcomes if outcome.checked and not outcome.ok]
    unchecked = [outcome for outcome in outcomes if not outcome.checked]

    for outcome in dead:
        status = outcome.status if outcome.status is not None else "no response"
        print(f"  - {outcome.url} -> {status} ({outcome.detail}), {outcome.attempts} attempt(s)")

    for outcome in unchecked:
        print(f"  · not checked: {outcome.url}")

    if unchecked:
        # Reported separately because it is a different fact. Folding these into the dead
        # count would claim the pages are gone; folding them into the live count would
        # claim they answered. Neither happened.
        print(
            f"\nSkills links FAILED: the sweep ran out of its time budget with "
            f"{len(unchecked)}/{len(urls)} URL(s) never checked"
            f"{f', and {len(dead)} dead among those it reached' if dead else ''}."
        )

        return 1

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
