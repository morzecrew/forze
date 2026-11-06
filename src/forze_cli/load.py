"""``forze load`` — drive a running app with a load plan, and compare two runs.

``run`` takes a ``module:attribute`` import string pointing at a
:class:`~forze_load.LoadPlan`, or a zero-argument callable returning one, and runs a closed
or open ramp, or a soak, against the plan's target. ``compare`` reads two run files. Both need the
``load`` extra; ``forze_load`` is imported inside the commands so ``forze --help`` works
without it.
"""

from __future__ import annotations

import asyncio
import sys
import traceback
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any, NoReturn

import typer

from forze.base.exceptions import CoreException
from forze_cli._compat import require_load
from forze_cli.loader import load_object

# ----------------------- #

load_app = typer.Typer(
    no_args_is_help=True,
    # No locals panel on unhandled errors (see the root app): a plan's frames hold the
    # credentials its principals signed in with.
    pretty_exceptions_show_locals=False,
    help=(
        "Load-test a running app: ramp it as several principals under a closed or open "
        "model, write JSON lines, compare two runs (needs the 'load' extra)."
    ),
)

# ....................... #


@load_app.callback()
def _ensure_extra() -> None:  # pyright: ignore[reportUnusedFunction]
    """Guard every ``load`` command on the load extra being installed."""

    require_load()


# ....................... #


def _numbers(spec: str) -> list[float]:
    parts = spec.split(",")

    # An empty item (`1,,4`, a trailing comma) is refused, not skipped: skipping it would run
    # a different ramp from the one typed.
    if any(not part.strip() for part in parts):
        raise typer.BadParameter(f"expected comma-separated numbers with none empty, got {spec!r}")

    try:
        return [float(part) for part in parts]

    except ValueError as error:
        raise typer.BadParameter(f"expected comma-separated numbers, got {spec!r}") from error


def _given(ctx: typer.Context, *names: str) -> list[str]:
    """The options among *names* typed on the command line, as ``--option-name``."""

    return [
        "--" + name.replace("_", "-")
        for name in names
        if (source := ctx.get_parameter_source(name)) is not None and source.name != "DEFAULT"
    ]


def _warn_unattributed(meta: Mapping[str, Any]) -> None:
    """Warn at the start of a soak whose first scrape ties no series to one process."""

    attribution = meta.get("server", {}).get("attribution")

    if attribution == "none":
        typer.echo(
            "warning: the metrics scrape names no instance, so no server series will be "
            "fitted; client numbers still are. Name each process with bootstrap_telemetry.",
            err=True,
        )

    elif attribution == "interleaved":
        # Series that name their own instance are still fitted; only the rest are at risk.
        typer.echo(
            "warning: the metrics scrape names several instances, so its unlabelled series "
            "may not be fitted; series labelled service_instance_id still are. Serve one "
            "scrape for every worker.",
            err=True,
        )


@load_app.command("run")
def run_command(
    ctx: typer.Context,
    ref: str = typer.Argument(
        ...,
        help="Import string of a LoadPlan, e.g. 'myapp.load:plan'.",
        metavar="MODULE:ATTR",
    ),
    model: str = typer.Option("closed", help="'closed' (workers) or 'open' (arrival rates)."),
    steps: str = typer.Option(
        "1,2,4,8,16,32",
        help="Comma-separated workers per step (closed) or arrivals per second (open).",
    ),
    step_s: float = typer.Option(30.0, help="Seconds per step, warm-up included."),
    warm_s: float = typer.Option(5.0, help="Seconds at the start of each step left out."),
    think_s: float = typer.Option(0.0, help="Closed model: pause after each answer."),
    stop_error_rate: float = typer.Option(
        0.05, help="Share of 5xx (504 included) and transport failures that stops the ramp."
    ),
    soak_s: float | None = typer.Option(
        None,
        help="Soak instead of ramp: hold the one load level --steps names for this many "
        "seconds, and flag drift.",
    ),
    window_s: float = typer.Option(60.0, help="Soak: seconds per window, one row each."),
    out: Path = typer.Option(Path("load.jsonl"), help="JSON lines file to write."),
) -> None:
    """Run a ramp or a soak against the plan's target and write one JSON line per step.

    Exits 0 when the run finishes, including a ramp that stopped on its stop rule: a stop is
    a result, recorded in the summary. Gate on it with ``forze load compare --fail-on``. A
    soak's drift flags never fail it. Exits 2 when the plan is refused, an operation it
    names does not resolve, a soak's metrics route cannot be read, a sign-in fails, a
    sample comes back empty, the run file cannot be written, or the plan's own code raises
    or exits; never 1, which is ``compare``'s crossed gate.
    """

    from forze_load import ClosedRamp, LoadPlan, OpenRamp, Soak, run, summarize

    try:
        target = load_object(ref)

        if callable(target) and not isinstance(target, LoadPlan):
            target = target()

    # A plan module or factory that builds a refused credential or profile is a refusal too.
    except CoreException as error:
        typer.echo(f"refused: {error.summary}", err=True)
        raise typer.Exit(code=2) from error

    except (ValueError, ImportError, AttributeError) as error:
        typer.echo(f"cannot load {ref!r}: {error}", err=True)
        raise typer.Exit(code=2) from error

    # Anything else a plan module or factory raises, or exits with, is a bug in the plan.
    except (Exception, SystemExit) as error:
        _plan_failed(error, "the plan's module or factory")

    if not isinstance(target, LoadPlan):
        typer.echo(
            f"{ref!r} is not a LoadPlan (got {type(target).__name__}). Expose one, or a "
            "zero-argument callable returning one.",
            err=True,
        )
        raise typer.Exit(code=2)

    values = _numbers(steps)

    if model == "closed" and not all(value.is_integer() for value in values):
        raise typer.BadParameter(f"closed-model steps are whole worker counts, got {steps!r}")

    if soak_s is not None and len(values) != 1:
        raise typer.BadParameter(f"a soak holds one load level: --steps takes one, got {steps!r}")

    # An option the chosen run does not read is refused, not ignored.
    if soak_s is None and _given(ctx, "window_s"):
        raise typer.BadParameter("--window-s sets a soak's windows: give --soak-s too")

    if soak_s is not None and (ramp_only := _given(ctx, "step_s", "warm_s")):
        raise typer.BadParameter(f"a soak takes --window-s, not {' or '.join(ramp_only)}")

    if model not in ("closed", "open"):
        raise typer.BadParameter("model must be 'closed' or 'open'")

    try:
        if soak_s is not None:
            profile: ClosedRamp | OpenRamp | Soak = Soak(
                duration_s=soak_s,
                window_s=window_s,
                workers=int(values[0]) if model == "closed" else None,
                rate=values[0] if model == "open" else None,
                think_s=think_s,
                stop_error_rate=stop_error_rate,
            )

        elif model == "closed":
            profile = ClosedRamp(
                steps=[int(value) for value in values],
                step_s=step_s,
                warm_s=warm_s,
                think_s=think_s,
                stop_error_rate=stop_error_rate,
            )

        else:
            profile = OpenRamp(
                rates=values, step_s=step_s, warm_s=warm_s, stop_error_rate=stop_error_rate
            )

        result = asyncio.run(run(target, profile, out=out, on_meta=_warn_unattributed))

    except CoreException as error:
        typer.echo(f"refused: {error.summary}", err=True)
        raise typer.Exit(code=2) from error

    # A scenario or hook of the plan raised, or exited: a bug in the plan, not a crossed
    # gate, so not 1. Refusals raised inside a task group, and the run file, are told apart.
    except (Exception, SystemExit) as error:
        if (refusal := _refusal(error)) is not None:
            typer.echo(f"refused: {refusal.summary}", err=True)
            raise typer.Exit(code=2) from error

        if (
            isinstance(error, OSError)
            and error.filename is not None
            and Path(error.filename) == out
        ):
            typer.echo(f"cannot write {out}: {error.strerror}", err=True)
            raise typer.Exit(code=2) from error

        _plan_failed(error, "the plan")

    for line in summarize(result.rows, result.summary):
        typer.echo(line)

    # A read sweep's routes it could not generate are listed, never guessed (RFC 0084 §5.5).
    for operation, reason in result.meta["plan"]["skipped"].items():
        typer.echo(f"skipped {operation}: {reason}")

    typer.echo(f"wrote {out}")


def _refusal(error: BaseException) -> CoreException | None:
    """The first refusal of a group made only of refusals, as a task group raises them."""

    if isinstance(error, BaseExceptionGroup):
        leaves = list(_leaves(error))
        refusals = [leaf for leaf in leaves if isinstance(leaf, CoreException)]

        if refusals and len(refusals) == len(leaves):
            return refusals[0]

    return None


def _leaves(group: BaseExceptionGroup[BaseException]) -> Iterator[BaseException]:
    for error in group.exceptions:
        if isinstance(error, BaseExceptionGroup):
            yield from _leaves(error)

        else:
            yield error


def _plan_failed(error: BaseException, where: str) -> NoReturn:
    """Exit 2 for a bug in *where*: 1 is a crossed ``--fail-on`` gate, and this is no verdict.

    The traceback has no locals: a plan's frames hold its principals' credentials.
    """

    if isinstance(error, SystemExit):
        typer.echo(f"{where} exited (status {error.code!r}); the run did not finish", err=True)

    else:
        traceback.print_exception(error, file=sys.stderr)
        typer.echo(f"{where} raised {type(error).__name__}; the run did not finish", err=True)

    raise typer.Exit(code=2) from error


@load_app.command("compare")
def compare_command(
    base: str = typer.Argument(..., help="Run file to compare against."),
    head: str = typer.Argument(..., help="Run file to compare."),
    fail_on: str | None = typer.Option(
        None,
        help="Exit 1 when a metric is worse by more than a share, e.g. 'p95:15%,goodput:10%'.",
    ),
) -> None:
    """Compare two run files step by step. Reports only, unless ``--fail-on`` is given."""

    from forze_load import compare_runs, parse_fail_on, read_run

    try:
        # Only an absent option disables the gate: `--fail-on "$UNSET"` is refused, not ignored.
        thresholds = parse_fail_on(fail_on) if fail_on is not None else None
        comparison = compare_runs(read_run(base), read_run(head), fail_on=thresholds)

    except CoreException as error:
        typer.echo(f"refused: {error.summary}", err=True)
        raise typer.Exit(code=2) from error

    for line in comparison.lines:
        typer.echo(line)

    for warning in comparison.warnings:
        typer.echo(f"warning: {warning}", err=True)

    for failure in comparison.failures:
        typer.echo(f"FAIL {failure}", err=True)

    if comparison.failures:
        raise typer.Exit(code=1)
