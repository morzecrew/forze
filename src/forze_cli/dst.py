"""``forze dst`` — run and inspect deterministic simulation against an app's operations.

Each command takes a ``module:attribute`` import string pointing at a
:class:`~forze_dst.Simulation`. The scenario is auto-derived (catalog + reactive probe) so
the common case needs no driver script — point at your registry-backed simulation and go.
"""

from __future__ import annotations

import typer

from forze.base.primitives import utcnow
from forze_cli._compat import require_dst
from forze_cli.loader import load_simulation
from forze_dst import (
    Constant,
    FaultPolicy,
    FaultRule,
    LatencyProfile,
    LatencyRule,
    RegressionEntry,
    SchedulerKind,
    SimulationConfig,
    Strategy,
    append_regression,
    entry_from_report,
    load_regressions,
)

# ----------------------- #

_DEFAULT_CORPUS = "dst-regressions.jsonl"

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


def _faults(fault_error: float) -> FaultPolicy | None:
    """A broad transient-error policy (all ports) from the CLI knob, or ``None``."""

    if fault_error <= 0.0:
        return None

    return FaultPolicy(rules=(FaultRule(error=fault_error),))


def _latency(latency: float) -> LatencyProfile | None:
    """A constant per-call latency (all ports) from the CLI knob, or ``None``."""

    if latency <= 0.0:
        return None

    return LatencyProfile(rules=(LatencyRule(dist=Constant(latency)),))


def _config(
    *,
    strategy: Strategy,
    seed_list: list[int],
    act_count: int,
    concurrency: int,
    max_examples: int,
    max_runs: int,
    pct: bool,
    depth: int,
    fault_error: float,
    latency: float,
) -> SimulationConfig:
    """Assemble a :class:`SimulationConfig` from the shared CLI knobs (run + replay)."""

    return SimulationConfig(
        strategy=strategy,
        seeds=seed_list,
        act_count=act_count,
        concurrency=concurrency,
        max_examples=max_examples,
        max_runs=max_runs,
        dpor_seed=seed_list[0] if seed_list else 0,
        scheduler=SchedulerKind.PCT if pct else SchedulerKind.RANDOM,
        pct_depth=depth,
        faults=_faults(fault_error),
        latency=_latency(latency),
    )


# ....................... #


@dst_app.command()
def run(
    target: str = typer.Argument(
        ..., help="Import string 'module:attr' of a Simulation."
    ),
    strategy: Strategy = typer.Option(Strategy.SCENARIO, help="Exploration strategy."),
    seeds: str = typer.Option("0-20", help="Seeds: 'N' | 'A-B' | 'a,b,c'."),
    act_count: int = typer.Option(8, help="Act operations per run."),
    concurrency: int = typer.Option(4, help="Max concurrent operations."),
    max_examples: int = typer.Option(200, help="Hypothesis: examples to try."),
    max_runs: int = typer.Option(500, help="DPOR: interleavings to explore."),
    pct: bool = typer.Option(False, help="Scenario strategy: use the PCT scheduler."),
    depth: int = typer.Option(3, help="PCT: target bug depth."),
    fault_error: float = typer.Option(
        0.0, help="Inject a transient error at every port with this probability [0,1]."
    ),
    latency: float = typer.Option(
        0.0, help="Inject this constant per-port latency (seconds of virtual time)."
    ),
    save_regression: bool = typer.Option(
        False, help="On a violation, append the seed to the regression corpus."
    ),
    regression_file: str = typer.Option(
        _DEFAULT_CORPUS, help="Regression corpus path (JSON Lines)."
    ),
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
    seed_list = _parse_seeds(seeds)

    report = sim.run(
        _config(
            strategy=strategy,
            seed_list=seed_list,
            act_count=act_count,
            concurrency=concurrency,
            max_examples=max_examples,
            max_runs=max_runs,
            pct=pct,
            depth=depth,
            fault_error=fault_error,
            latency=latency,
        ),
        scenario=scenario,
    )

    if report is None:
        typer.echo("✓ no violation found")
        return

    typer.echo(report.format())

    if save_regression:
        append_regression(
            regression_file,
            entry_from_report(
                report,
                target=target,
                found_at=utcnow().isoformat(),
            ),
        )
        typer.echo(f"\n↳ saved seed {report.seed} to {regression_file}")

    raise typer.Exit(code=1)


# ....................... #


@dst_app.command()
def coverage(
    target: str = typer.Argument(
        ..., help="Import string 'module:attr' of a Simulation."
    ),
    seeds: str = typer.Option("0-200", help="Seed pool: 'N' | 'A-B' | 'a,b,c'."),
    act_count: int = typer.Option(8, help="Act operations per run."),
    concurrency: int = typer.Option(4, help="Max concurrent operations."),
    plateau: int = typer.Option(
        8, help="Stop after this many consecutive seeds add no new behavior (0 = full sweep)."
    ),
    fault_error: float = typer.Option(0.0, help="Transient-error probability per port."),
    latency: float = typer.Option(0.0, help="Constant per-port latency (virtual seconds)."),
) -> None:
    """Coverage-guided sweep: explore until behavior saturates; print a coverage report.

    Runs seeds while new behavior keeps appearing and stops once it plateaus, so the pool
    right-sizes itself. Prints how much behavior was covered and which seeds mattered; exits 1
    if the sweep hit an invariant violation (printing the minimized counterexample too).
    """

    sim = load_simulation(target)
    seed_list = _parse_seeds(seeds)

    stats = sim.coverage(
        SimulationConfig(
            strategy=Strategy.SCENARIO,
            seeds=seed_list,
            act_count=act_count,
            concurrency=concurrency,
            coverage_plateau=plateau,
            faults=_faults(fault_error),
            latency=_latency(latency),
        ),
        scenario=sim.derive_scenario(),
    )

    typer.echo(stats.format())

    if stats.violation is not None:
        typer.echo("")
        typer.echo(stats.violation.format())
        raise typer.Exit(code=1)


# ....................... #


@dst_app.command()
def replay(
    target: str = typer.Option(
        "", help="Override the app for every seed; default replays each entry's saved target."
    ),
    regression_file: str = typer.Option(
        _DEFAULT_CORPUS, help="Regression corpus path (JSON Lines)."
    ),
    strategy: Strategy = typer.Option(Strategy.SCENARIO, help="Exploration strategy."),
    act_count: int = typer.Option(8, help="Act operations per run."),
    concurrency: int = typer.Option(4, help="Max concurrent operations."),
    max_examples: int = typer.Option(200, help="Hypothesis: examples to try."),
    max_runs: int = typer.Option(500, help="DPOR: interleavings to explore."),
    pct: bool = typer.Option(False, help="Scenario strategy: use the PCT scheduler."),
    depth: int = typer.Option(3, help="PCT: target bug depth."),
    fault_error: float = typer.Option(0.0, help="Transient-error probability per port."),
    latency: float = typer.Option(0.0, help="Constant per-port latency (virtual seconds)."),
) -> None:
    """Re-run every saved regression seed; exit 1 if any still violates (the CI guard).

    Replay each corpus seed against its app (or *target* if given) with the same exploration
    knobs used to find it. A seed that still violates is a live (or regressed) bug — printed
    and counted toward a non-zero exit. A changed registry fingerprint is flagged (the saved
    seed may no longer reproduce the original path).
    """

    entries = load_regressions(regression_file)

    if not entries:
        typer.echo(f"✓ no regression seeds in {regression_file}")
        return

    grouped: dict[str, list[RegressionEntry]] = {}
    for entry in entries:
        chosen = target or entry.target
        if not chosen:
            typer.echo(
                f"⚠ seed {entry.seed} has no saved target and none was given — skipping"
            )
            continue
        grouped.setdefault(chosen, []).append(entry)

    failures = 0
    checked = 0

    for app, group in grouped.items():
        sim = load_simulation(app)
        fingerprint = sim.fingerprint()
        scenario = sim.derive_scenario()

        for entry in group:
            checked += 1

            if (
                entry.registry_fingerprint
                and entry.registry_fingerprint != fingerprint
            ):
                typer.echo(
                    f"⚠ seed {entry.seed}: registry changed since saved — replay may "
                    "not reproduce the original path"
                )

            report = sim.run(
                _config(
                    strategy=strategy,
                    seed_list=[entry.seed],
                    act_count=act_count,
                    concurrency=concurrency,
                    max_examples=max_examples,
                    max_runs=max_runs,
                    pct=pct,
                    depth=depth,
                    fault_error=fault_error,
                    latency=latency,
                ),
                scenario=scenario,
            )

            if report is not None:
                failures += 1
                typer.echo(report.format())

    if failures:
        typer.echo(f"\n✗ {failures}/{checked} regression seed(s) still violate")
        raise typer.Exit(code=1)

    typer.echo(f"✓ {checked} regression seed(s) clean")


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
