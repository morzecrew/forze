"""In-memory authn event sink for test inspection."""

from __future__ import annotations

from typing import final

import attrs

from forze.application.contracts.authn import AuthnEvent, AuthnEventSink
from forze_mock.state import MockState

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RecordingAuthnEventSink(AuthnEventSink):
    """Append every authn event to :attr:`MockState.authn_events`.

    Registered by :class:`~forze_mock.execution.MockDepsModule` when
    ``authn_events=True`` (optional, like the real module's ``events`` knob);
    tests inspect ``state.authn_events`` seed-style for the recorded kinds and
    fields. Events carry only the login digest — asserting that
    ``login_digest != raw login`` against this sink is the standard privacy
    check.
    """

    state: MockState
    """Shared mock state the events are appended to."""

    # ....................... #

    async def record(self, event: AuthnEvent) -> None:
        with self.state.lock:
            self.state.authn_events.append(event)
