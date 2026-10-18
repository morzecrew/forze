"""Document dependency keys and routers."""

from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from pydantic import BaseModel

from forze.domain.models import BaseDTO, Document

from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import DocumentCommandPort, DocumentQueryPort
from .specs import DocumentSpec

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #

DocumentQueryDepPort = ConfigurableDepPort[
    DocumentSpec[R, Any, Any, Any],
    DocumentQueryPort[R],
]

DocumentCommandDepPort = ConfigurableDepPort[
    DocumentSpec[R, D, C, U],
    DocumentCommandPort[R, D, C, U],
]

# ....................... #

DocumentQueryDepKey = DepKey[DocumentQueryDepPort[Any]]("document_query")
"""Key used to register the ``DocumentQueryDepPort`` implementation."""

DocumentCommandDepKey = DepKey[DocumentCommandDepPort[Any, Any, Any, Any]]("document_command")
"""Key used to register the ``DocumentCommandDepPort`` implementation."""


class DocumentCommandHookDepPort(Protocol):
    """Wraps the command port resolved for one document."""

    def __call__(
        self,
        ctx: "ExecutionContext",
        port: DocumentCommandPort[Any, Any, Any, Any],
    ) -> DocumentCommandPort[Any, Any, Any, Any]: ...  # pragma: no cover


DocumentCommandHookDepKey = DepKey[DocumentCommandHookDepPort]("document_command_hook")
"""Key of a hook around a document's command port, registered routed under the document's name.

Every command port resolved for that document passes through it, whichever backend serves the
document, before the port is cached or wrapped further. A write that does not go through the
document's command port (raw SQL, another spec over the same table) does not."""

# ....................... #


class DocumentDeps(ConvenientDeps):
    """Convenience wrapper for document dependencies."""

    def query(self, spec: DocumentSpec[R, Any, Any, Any]) -> DocumentQueryPort[R]:
        """Resolve a document query port for the given spec."""

        return self._resolve_configurable(
            DocumentQueryDepKey,
            spec,
            route=spec.name,
        )

    # ....................... #

    def command(
        self,
        spec: DocumentSpec[R, D, C, U],
    ) -> DocumentCommandPort[R, D, C, U]:
        """Resolve a document command port for the given spec."""

        return self._resolve_command(
            DocumentCommandDepKey,
            spec,
            route=spec.name,
        )
