"""Hub search SQL name constants."""

from typing import Final

# ----------------------- #

HUB_CTE: Final[str] = "hf"
HUB_ROW_ALIAS: Final[str] = "h"
COMBO_ALIAS: Final[str] = "comb"
COMBO_TOP_RELATION: Final[str] = "combo_top"
HUB_RANK: Final[str] = "_hub_rank"
LEG_SCORE: Final[str] = "s"
LEG_EID: Final[str] = "eid"

# Groonga v2 needs physical row ids: projected when a pgroonga leg uses same_heap_as_hub.
HUB_GROONGA_TABLEOID: Final[str] = "_hub_groonga_tableoid"
HUB_GROONGA_CTID: Final[str] = "_hub_groonga_ctid"

HUB_INTERNAL_ROW_KEYS: Final[frozenset[str]] = frozenset(
    {
        HUB_RANK,
        HUB_GROONGA_TABLEOID,
        HUB_GROONGA_CTID,
        LEG_SCORE,
        LEG_EID,
    },
)
