# config/constants.py
from typing import Tuple, Dict, List, Set

APP_TITLE = "Buffertpallar → Order-allokering (GUI) — 5.0"
DEFAULT_OUTPUT = "allocated_orders.csv"

INVALID_LOC_PREFIXES: Tuple[str, ...] = ("AA",)
INVALID_LOC_EXACT: set[str] = {"TRANSIT", "TRANSIT_ERROR", "MISSING", "UT2"}

# Allokering använder 29/30/32
ALLOC_BUFFER_STATUSES: set[int] = {29, 30, 32}
# Refill använder 29/30
REFILL_BUFFER_STATUSES: set[int] = {29, 30}

NEAR_MISS_PCT: float = 0.15  # 15% över behov
