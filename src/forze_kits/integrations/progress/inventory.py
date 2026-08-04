"""What the progress plane contributes to an application's spec inventory.

Neither half of this plane is written by an application's author: the job collection comes
out of :func:`~.record.job_record_spec` and the transitions route out of
:func:`~.reporter.progress_outbox_spec`, so both are exactly the shape reconciliation exists
to catch — bound at runtime, catalogued nowhere, and therefore invisible to an export, which
cannot tell a plane that carries nothing from a plane nobody remembered.

Only what you actually wired belongs here. A catalogued route that no dependency binds is a
hard startup failure (a port that can never resolve), so this registers exactly the specs it
is given rather than defaulting them in: an application that keeps the record but not the
realtime lane passes only ``spec``, and one whose projector runs on a different node than its
reporters contributes a different half on each.
"""

from forze.application.contracts.inventory import SpecRegistry, SpecSource
from forze.application.contracts.outbox import OutboxSpec
from forze.application.contracts.realtime import RealtimeSignal
from forze.base.exceptions import exc

from .record import JobDocumentSpec

# ----------------------- #


def progress_spec_contributions(
    *,
    spec: JobDocumentSpec | None = None,
    outbox_spec: OutboxSpec[RealtimeSignal] | None = None,
) -> SpecRegistry:
    """Catalogue the progress specs this deployment binds — merge it at assembly.

    The same contract as ``AggregateKit.spec_contributions`` and
    ``RealtimeTransport.spec_contributions``::

        registry = SpecRegistry().register(*my_specs).merge(
            progress_spec_contributions(
                spec=job_record_spec(), outbox_spec=progress_outbox_spec()
            )
        )

    The job collection lands **exportable** by the document plane's own default, which is the
    right answer for it: nothing recomputes a job record, and it is not in-flight work a
    quiesce could drain — the history of what ran is only where it was written. The
    transitions route is an ordinary outbox and is drained like any other.

    :raises exc.configuration: If neither spec is given (a contribution of nothing is a
        mistake worth failing on — it reads exactly like the omission it would cause).
    """

    if spec is None and outbox_spec is None:
        raise exc.configuration(
            "progress_spec_contributions() was given neither the job collection nor the "
            "transitions route, so it would contribute nothing — pass the specs this "
            "deployment actually wires (spec=job_record_spec(), and/or "
            "outbox_spec=progress_outbox_spec()).",
            code="progress_spec_contributions_empty",
        )

    registry = SpecRegistry()

    if spec is not None:
        registry.register(spec, source=SpecSource.KIT)

    if outbox_spec is not None:
        registry.register(outbox_spec, source=SpecSource.KIT)

    return registry
