"""``forze dst`` — run and inspect deterministic simulation against an app's operations.

Each command takes a ``module:attribute`` import string pointing at a
:class:`~forze_dst.Simulation`. The scenario is auto-derived (catalog + reactive probe) so
the common case needs no driver script — point at your registry-backed simulation and go.
"""

from __future__ import annotations

from enum import Enum

import typer

from forze_cli._compat import require_dst
from forze_cli.loader import load_simulation
from forze_dst import pct_scheduler_factory

# ----------------------- #

dst_app = typer.Typer(
    no_args_is_help=True,
    help=(
        "Deterministic simulation testing — explore an app's operations for concurrency "
        "and consistency bugs (needs the 'dst' extra)."
    ),
)

# ....................... #


@dst_app.callback()
def _ensure_extra() -> None:  # pyright: ignore[reportUnusedFunction]
    """Guard every ``dst`` command on the DST extra being installed."""

    require_dst()


# ....................... #


class Strategy(str, Enum):
    """Exploration strategy for ``dst run``."""

    scenario = "scenario"
    hypothesis = "hypothesis"
    dpor = "dpor"


# ....................... #


def _parse_seeds(spec: str) -> list[int]:
    """Parse ``"20"`` → range(20), ``"3-7"`` → 3..7, ``"1,4,9"`` → those seeds."""

    spec = spec.strip()

    if "," in spec:
        return [int(part) for part in spec.split(",") if part.strip()]

    if "-" in spec:
        low, _, high = spec.partition("-")
        return list(range(int(low), int(high) + 1))

    return list(range(int(spec)))


# ....................... #


@dst_app.command()
def run(
    target: str = typer.Argument(
        ..., help="Import string 'module:attr' of a Simulation."
    ),
    strategy: Strategy = typer.Option(Strategy.scenario, help="Exploration strategy."),
    seeds: str = typer.Option("0-20", help="Seeds: 'N' | 'A-B' | 'a,b,c'."),
    act_count: int = typer.Option(8, help="Act operations per run."),
    concurrency: int = typer.Option(4, help="Max concurrent operations."),
    max_examples: int = typer.Option(200, help="Hypothesis: examples to try."),
    max_runs: int = typer.Option(500, help="DPOR: interleavings to explore."),
    pct: bool = typer.Option(False, help="Scenario strategy: use the PCT scheduler."),
    depth: int = typer.Option(3, help="PCT: target bug depth."),
) -> None:
    """Explore an auto-derived scenario; print the counterexample (exit 1 if one is found)."""

    sim = load_simulation(target)

    if not sim.invariants:
        # No invariants → DST has nothing to assert (e.g. an ad-hoc bare registry). Say so
        # rather than printing a misleading "no violation found".
        typer.echo(
            "⚠ no invariants defined — nothing to check. Point at a Simulation that "
            "declares invariants (a bare registry has none) to actually find bugs."
        )
        return

    scenario = sim.derive_scenario()

    if strategy is Strategy.hypothesis:
        report = sim.explore_scenario_hypothesis(
            scenario,
            max_act=act_count,
            concurrency=concurrency,
            max_examples=max_examples,
        )

    elif strategy is Strategy.dpor:
        seed_list = _parse_seeds(seeds)
        report = sim.explore_scenario_dpor(
            scenario,
            act_count=act_count,
            concurrency=concurrency,
            seed=seed_list[0] if seed_list else 0,
            max_runs=max_runs,
        )

    else:
        report = sim.explore_scenario(
            scenario,
            act_count=act_count,
            concurrency=concurrency,
            seeds=_parse_seeds(seeds),
            scheduler_factory=(pct_scheduler_factory(depth=depth) if pct else None),
        )

    if report is None:
        typer.echo("✓ no violation found")
        return

    typer.echo(report.format())
    raise typer.Exit(code=1)


# ....................... #


@dst_app.command()
def topology(
    target: str = typer.Argument(
        ..., help="Import string 'module:attr' of a Simulation."
    ),
) -> None:
    """Print the recovered reactive cascade topology (who triggers whom, via which events)."""

    sim = load_simulation(target)
    typer.echo(sim.reactive_map().format())


# ....................... #


@dst_app.command()
def derive(
    target: str = typer.Argument(
        ...,
        help="Import string 'module:attr' of a Simulation.",
    ),
) -> None:
    """Print the auto-derived scenario — the inferred arrange and act rules."""

    sim = load_simulation(target)
    scenario = sim.derive_scenario()

    arrange = ", ".join(rule.op for rule in scenario.arrange)
    lines = ["derived scenario:", f"  arrange: {arrange or '(none)'}", "  act:"]

    if not scenario.act:
        lines.append("    (none)")

    for rule in scenario.act:
        requires = f"  requires {sorted(rule.requires)}" if rule.requires else ""
        lines.append(f"    {rule.op}{requires}")

    typer.echo("\n".join(lines))
