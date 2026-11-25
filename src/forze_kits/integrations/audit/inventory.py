"""What the audit trail contributes to an application's spec inventory.

The audit collection comes out of :func:`~.record.audit_record_spec`, not the application's
author, so forze owns its shape: the application binds it to a table and cannot change its
columns. It is catalogued as :attr:`~forze.application.contracts.inventory.SpecSource.FRAMEWORK`,
which also lets ``forze schema`` print its table (RFC 0080 §5.7).
"""

from forze.application.contracts.inventory import SpecRegistry, SpecSource

from .record import AuditDocumentSpec

# ----------------------- #


def audit_spec_contributions(spec: AuditDocumentSpec) -> SpecRegistry:
    """Catalogue the audit collection this deployment binds — merge it at assembly::

        registry = SpecRegistry().register(*my_specs).merge(
            audit_spec_contributions(audit_record_spec())
        )

    Pass the spec the deployment wires, its name and encryption included: a catalogued route
    that no dependency binds refuses startup.
    """

    return SpecRegistry().register(spec, source=SpecSource.FRAMEWORK)
