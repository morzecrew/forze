"""After-commit capability effect runner (replaces nested closure)."""

from typing import Any

import attrs

from forze.application._logger import logger

from .async_util import maybe_await
from .trace import (
    CapabilitySkip,
    CapabilityStore,
    SchedulableCapabilitySpec,
    capability_step_label,
)

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CapabilityAfterCommitRunner:
    """Runs after-commit effects with shared capability store and tracing."""

    store: CapabilityStore
    """Capability store."""

    effects: tuple[Any, ...]
    """After-commit effects."""

    specs: tuple[SchedulableCapabilitySpec, ...]
    """After-commit specs."""

    bucket_label: str = "after_commit"
    """Bucket label."""

    # ....................... #

    async def __call__(self, args: Any, res: Any) -> Any:
        for eff, spec in zip(self.effects, self.specs, strict=True):
            label = capability_step_label(spec, eff)

            if not self.store.is_ready(spec.requires):
                logger.debug(
                    "Skipping after_commit effect (missing capability): label=%s",
                    label,
                )

                self.store.record_execution(
                    bucket=self.bucket_label,
                    spec=spec,
                    impl=eff,
                    kind="after_commit",
                    action="skipped_missing",
                    detail=None,
                )

                continue

            logger.debug(
                "Running after_commit effect: label=%s",
                label,
            )

            raw = eff(args, res)
            out = await maybe_await(raw)

            if isinstance(out, CapabilitySkip):
                self.store.mark_missing(spec.provides)

                self.store.record_execution(
                    bucket=self.bucket_label,
                    spec=spec,
                    impl=eff,
                    kind="after_commit",
                    action="skipped_return",
                    detail=out.reason,
                )

            else:
                res = out
                self.store.mark_success(spec.provides)

                self.store.record_execution(
                    bucket=self.bucket_label,
                    spec=spec,
                    impl=eff,
                    kind="after_commit",
                    action="ran",
                    detail=None,
                )

        return res
