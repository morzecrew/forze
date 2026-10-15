from .leadership import (
    LeadershipSpec,
    Term,
    TermEnd,
    instrument_leadership,
    leadership_lifecycle_step,
)
from .singleton import singleton_lifecycle_step

# ----------------------- #

# The background-loop machinery (BackgroundLoopControl, run_supervised) lives in
# forze.application.execution.background — it is core execution machinery satisfying the
# core DrainableLoop protocol, and edge packages that cannot import kits need it too.
__all__ = [
    "LeadershipSpec",
    "Term",
    "TermEnd",
    "instrument_leadership",
    "leadership_lifecycle_step",
    "singleton_lifecycle_step",
]
