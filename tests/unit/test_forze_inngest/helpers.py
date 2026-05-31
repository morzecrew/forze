from collections.abc import Sequence

import attrs
import inngest

from forze_inngest.kernel.client import InngestClientPort

# ----------------------- #


@attrs.define(slots=True)
class RecordingInngestClient(InngestClientPort):
    """Records events passed to :meth:`send`."""

    sent: list[inngest.Event] = attrs.field(factory=list)
    _sdk: inngest.Inngest = attrs.field(
        factory=lambda: inngest.Inngest(app_id="forze-test"),
    )

    @property
    def native(self) -> inngest.Inngest:
        return self._sdk

    async def send(
        self,
        events: inngest.Event | Sequence[inngest.Event],
    ) -> list[str]:
        if isinstance(events, inngest.Event):
            batch = [events]

        else:
            batch = list(events)

        self.sent.extend(batch)
        return [f"id-{len(self.sent)}"]
