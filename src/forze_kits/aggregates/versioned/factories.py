"""Factories for versioned-facts document registries."""

from typing import Any, TypeVar

from forze.application.contracts.document import DocumentSpec
from forze.application.execution.operations import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.primitives import StrKeyNamespace
from forze_kits.aggregates.document.value_objects import DocumentDTOs
from forze_kits.domain.versioned.models import (
    DocWithVersioning,
    UpdateCmdWithVersioning,
)
from forze_kits.dto.paginated import Paginated

from .dto import CorrectDocumentDTO, FactAsOfDTO, FactIdDTO
from .handlers import CorrectDocument, FactAsOf, FactHistory
from .operations import VersionedKernelOp
from .policy import VersionedPolicy

# ----------------------- #


def _parametrized(generic: Any, arg: Any) -> Any:
    """Parametrize a generic envelope with a runtime model type.

    Kept off the static-type path: the models are only known at build time, so the subscription
    happens on values rather than as an annotation.
    """

    return generic[arg]


# ....................... #

D = TypeVar("D", bound=DocWithVersioning)
U = TypeVar("U", bound=UpdateCmdWithVersioning)

# ....................... #


def build_versioned_registry(
    spec: DocumentSpec[Any, D, Any, U],
    policy: VersionedPolicy,
    *,
    dtos: DocumentDTOs[Any, Any, Any] | None = None,
    ns: StrKeyNamespace | None = None,
) -> OperationRegistry:
    """Build the correction command and the two lineage reads for *spec*.

    The lineage-seeding CREATE is not here: it *overrides* an operation the document factory
    already registered, so it belongs with the other override in
    :meth:`~forze_kits.aggregates.versioned.wiring.VersionedWiring.bind`.

    :param spec: The versioned document specification.
    :param policy: Where correction records are stored.
    :param dtos: Inbound DTOs, when they are not the spec's own commands.
    :param ns: Optional namespace.
    :returns: Operation registry with CORRECT, HISTORY and AS_OF.
    """

    ns = ns or spec.default_namespace

    if spec.write is None or not spec.supports_update():
        return OperationRegistry()

    corrections = policy.corrections

    reg = OperationRegistry(
        handlers={
            ns.key(VersionedKernelOp.CORRECT): lambda ctx: CorrectDocument(
                doc=ctx.doc.command(spec),
                query=ctx.doc.query(spec),
                corrections=ctx.doc.command(corrections),
                create_cmd=spec.write["create_cmd"],
                actor=ctx.inv_ctx.get_authn,
            ),
            ns.key(VersionedKernelOp.HISTORY): lambda ctx: FactHistory(
                query=ctx.doc.query(spec),
            ),
            ns.key(VersionedKernelOp.AS_OF): lambda ctx: FactAsOf(
                query=ctx.doc.query(spec),
            ),
        },
    )

    return reg.set_descriptors(
        {
            VersionedKernelOp.CORRECT: OperationDescriptor(
                input_type=_parametrized(CorrectDocumentDTO, spec.write["update_cmd"]),
                output_type=spec.read,
                description="Supersede the current version of a fact with a corrected one.",
                sensitive=spec.sensitive,
            ),
            VersionedKernelOp.HISTORY: OperationDescriptor(
                input_type=FactIdDTO,
                output_type=_parametrized(Paginated, spec.read),
                description="Every version of a fact, oldest first.",
                sensitive=spec.sensitive,
            ),
            VersionedKernelOp.AS_OF: OperationDescriptor(
                input_type=FactAsOfDTO,
                output_type=spec.read,
                description="The version of a fact that was current at an instant.",
                sensitive=spec.sensitive,
            ),
        },
        namespace=ns,
    )
