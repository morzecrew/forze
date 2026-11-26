"""What the proposal collection contributes to an application's spec inventory.

The collection's spec comes out of :func:`~.record.proposal_spec`, but its items are the
application's models, so the table is the application's: it is catalogued
:attr:`~forze.application.contracts.inventory.SpecSource.KIT`, as a kit catalogues every spec it
derives from the application's (RFC 0080 row 28). ``forze schema --specs`` therefore does not
print it; the application migrates it as it does its own tables.
"""

from typing import Any

from forze.application.contracts.inventory import SpecRegistry, SpecSource

from .record import ProposalSpec

# ----------------------- #


def proposal_spec_contributions(spec: ProposalSpec[Any, Any]) -> SpecRegistry:
    """Catalogue the proposal collection this deployment binds — merge it at assembly::

        registry = SpecRegistry().register(*my_specs).merge(proposal_spec_contributions(spec))

    Pass the spec the deployment wires: a catalogued route that no dependency binds refuses
    startup.
    """

    return SpecRegistry().register(spec.document, source=SpecSource.KIT)
