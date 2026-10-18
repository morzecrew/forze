"""A hook registered under a document's name wraps every command port resolved for it.

# covers: forze.application.execution.deps.port_instrumentation (maybe_wrap_document_hook),
#         forze.application.execution.deps.frozen (resolve_configurable)
"""

from __future__ import annotations

from typing import Any

import attrs
import pytest

from forze.application.contracts.deps import backend_of
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentCommandHookDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.unit

# ----------------------- #


class _Doc(Document):
    pass


class _Read(ReadDocument):
    pass


HOOKED = DocumentSpec(
    name="hooked",
    read=_Read,
    write={"domain": _Doc, "create_cmd": CreateDocumentCmd, "update_cmd": CreateDocumentCmd},
)
PLAIN = DocumentSpec(
    name="plain",
    read=_Read,
    write={"domain": _Doc, "create_cmd": CreateDocumentCmd, "update_cmd": CreateDocumentCmd},
)


@attrs.define(eq=False)
class _Port:
    spec: Any
    tenant_aware: bool = False


@attrs.define(eq=False)
class _Hooked:
    inner: Any
    calls: list[Any]


def test_only_the_named_documents_command_port_is_hooked_and_cached_hooked() -> None:
    calls: list[Any] = []

    def hook(ctx: Any, port: Any) -> Any:
        calls.append(port)
        return _Hooked(inner=port, calls=calls)

    ctx = context_from_deps(
        Deps.plain(
            {
                DocumentCommandDepKey: lambda ctx, spec: _Port(spec=spec),
                DocumentQueryDepKey: lambda ctx, spec: _Port(spec=spec),
            }
        ).merge(Deps.routed({DocumentCommandHookDepKey: {HOOKED.name: hook}}))
    )

    hooked = backend_of(ctx.doc.command(HOOKED))
    assert isinstance(hooked, _Hooked) and hooked.inner.spec is HOOKED

    # Resolved again, the cached port is the hooked one; the hook ran once.
    assert backend_of(ctx.doc.command(HOOKED)) is hooked
    assert len(calls) == 1

    assert isinstance(backend_of(ctx.doc.command(PLAIN)), _Port)
    assert isinstance(backend_of(ctx.doc.query(HOOKED)), _Port)
