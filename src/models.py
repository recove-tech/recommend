from typing import List, Dict, Any

from dataclasses import dataclass, field
from enum import Enum


class InteractionType(Enum):
    CLICK_OUT = "click_out"
    SAVED = "saved"


@dataclass
class Vector:
    id: str
    values: List[float]
    metadata: Dict[str, Any]


@dataclass
class BigQueryRow:
    id: str
    created_at: str
    user_id: str
    item_id: str


@dataclass
class SupabaseRow:
    user_id: str
    item_id: str
    point_id: str