"""Shared tenant/namespace-scoped queue-name resolution for queue adapters."""

from typing import ClassVar

import attrs

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    resolve_scoped_namespace,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.primitives import OnceCell

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ScopedQueueNamingMixin(TenancyMixin):
    """Tenant- and namespace-scoped physical queue-name resolution.

    Queue adapters share one naming scheme — ``[tenant prefix][namespace]queue``
    joined by a backend-specific separator (``":"`` for RabbitMQ, ``"-"`` for
    SQS). Backends set :attr:`queue_name_separator` (and optionally
    :attr:`queue_backend_label` for error messages) and call
    :meth:`_scoped_queue_name`; backend-specific guards (for example the SQS
    absolute-URL rejection) stay in the backend adapter.
    """

    namespace: NamedResourceSpec = ""
    """Queue namespace (static name or tenant-scoped resolver)."""

    _namespace_cell: OnceCell[str] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )

    queue_name_separator: ClassVar[str] = ":"
    """Separator joining tenant prefix, namespace, and queue name."""

    queue_backend_label: ClassVar[str] = "queue"
    """Backend label used in error messages (e.g. ``"SQS queue"``)."""

    # ....................... #

    async def _resolved_namespace(self) -> str:
        return await resolve_scoped_namespace(
            self.namespace,
            tenant_id=self._tenant_id_for_resolve(),
            cell=self._namespace_cell,
        )

    # ....................... #

    async def _scoped_queue_name(self, queue: str) -> str:
        """Resolve *queue* to its physical, tenant/namespace-prefixed name."""

        separator = self.queue_name_separator
        tenant_id = self.require_tenant_if_aware()

        if tenant_id is not None:
            tenant_prefix = f"tenant{separator}{tenant_id}"

        else:
            tenant_prefix = ""

        namespace = await self._resolved_namespace()

        if namespace:
            namespaced_queue = f"{namespace}{separator}{queue}"

        else:
            namespaced_queue = queue

        return f"{tenant_prefix}{separator}{namespaced_queue}".lstrip(separator)
