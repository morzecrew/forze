from typing import Any, Callable

import attrs
from structlog.typing import EventDict, WrappedLogger

from .normalization import normalize_event_dict
from .rich import render_event

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ForzeConsoleRenderer:
    """Render *event_dict* as ``ts  LEVEL  [logger]  event  |  extra``."""

    colors: bool = True
    """Enable colors."""

    logger_name_width: int = 22
    """Width of the logger name."""

    message_width: int = 100
    """Width of the message (event)."""

    max_traceback_lines: int = 18
    """Maximum number of traceback lines to render."""

    sep_width: int = attrs.field(default=1, validator=attrs.validators.ge(1))
    """Width of the separator between the parts."""

    aliases: dict[str, str] = {
        "correlation_id": "corr",
        "execution_id": "exec",
        "causation_id": "caus",
        "operation_id": "op",
    }
    """Aliases for extra keys (will be replaced by the alias)."""

    transforms: dict[str, Callable[[Any], str]] = {
        "correlation_id": lambda value: str(value)[-6:],
        "execution_id": lambda value: str(value)[-6:],
        "causation_id": lambda value: str(value)[-6:],
    }
    """Transforms for extra keys (will be applied to the value)."""

    # ....................... #

    def __call__(self, _: WrappedLogger, __: str, event_dict: EventDict) -> str:
        ev = normalize_event_dict(
            event_dict,
            max_traceback_lines=self.max_traceback_lines,
        )

        return render_event(
            ev,
            colors=self.colors,
            logger_name_width=self.logger_name_width,
            message_width=self.message_width,
            sep_width=self.sep_width,
            aliases=self.aliases,
            transforms=self.transforms,
        )
