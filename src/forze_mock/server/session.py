"""The handle a control plane needs — and the reason it cannot exist for a real runtime."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, final

import attrs

from forze.base.exceptions import exc

if TYPE_CHECKING:
    from forze.application.execution import ExecutionRuntime
    from forze_mock.adapters import MockState
    from forze_mock.seeding import SeedPlan

    from .clock import ControlledTimeSource
    from .declaration import EmitHook
    from .faults import FaultBoard

# ----------------------- #

_ISSUER = object()
"""Handed to :class:`MockSession` by the server builder and by nothing else.

The control plane takes a session, not a flag, so "attach the control plane" cannot be
switched on for a production runtime by passing ``True`` — you would need one of these, and
only ``build_mock_server`` can mint one. A boolean would have made a reset/fault/state API
one config mistake away from a deployed service.
"""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockSession:
    """Everything the control plane operates on, for one served mock app."""

    issued_by: Any = attrs.field(repr=False)
    """Must be the module's issuer token — see :data:`_ISSUER`."""

    runtime: ExecutionRuntime
    """The composed runtime: mock as fallback, plus whatever real modules were declared."""

    state: MockState
    """The store every mock adapter shares — what ``reset`` clears and ``state`` inspects."""

    board: FaultBoard
    """Armed faults and latencies."""

    clock: ControlledTimeSource
    """The server-wide time source ``POST /_mock/time`` drives."""

    seed: SeedPlan | None = None
    """The plan applied at startup and re-applied by ``reset``."""

    on_emit: EmitHook | None = None
    """The app's realtime egress, when it supplied one."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.issued_by is not _ISSUER:
            raise exc.configuration(
                "A MockSession can only be created by forze_mock.server.build_mock_server — "
                "the control plane is reachable from a mock runtime and from nothing else"
            )


# ....................... #


def issue_session(**fields: Any) -> MockSession:
    """Mint a session. Internal to the server package."""

    return MockSession(issued_by=_ISSUER, **fields)
