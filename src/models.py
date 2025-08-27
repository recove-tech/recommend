from typing import Any, List, Dict

from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
from uuid import uuid4


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


@dataclass
class PineconeUsageEntry:
    read_units: int

    @classmethod
    def from_response(
        cls,
        response: Any,
    ) -> "PineconeUsageEntry":
        return cls(
            read_units=response.usage.read_units,
        )


@dataclass
class PineconeUsage:
    entries: List[PineconeUsageEntry] = field(default_factory=list)

    def __post_init__(self):
        self.id = str(uuid4())
        self.created_at = datetime.now().isoformat()
        self.method = "recommend"

    def add(self, entry: PineconeUsageEntry) -> None:
        self.entries.append(entry)

    @property
    def read_units(self) -> int:
        return sum(entry.read_units for entry in self.entries)

    @property
    def is_empty(self) -> bool:
        return len(self.entries) == 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": str(uuid4()),
            "created_at": datetime.now().isoformat(),
            "method": self.method,
            "read_units": self.read_units,
        }
