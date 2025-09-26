from typing import Any, List, Dict

from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
from uuid import uuid4


class InteractionType(Enum):
    CLICK_OUT = "click_out"
    SAVED = "saved"


@dataclass
class Subscription:
    user_id: str
    created_at: str
    status: str
    expire_at: str | None = None

    def __post_init__(self):
        if not isinstance(self.created_at, str):
            self.created_at = self.created_at.isoformat()
        if self.expire_at and not isinstance(self.expire_at, str):
            self.expire_at = self.expire_at.isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "user_id": self.user_id,
            "created_at": self.created_at,
            "status": self.status,
            "expire_at": self.expire_at,
        }

    @classmethod
    def from_supabase(cls, data: Dict) -> "Subscription":
        return cls(
            user_id=data["user_id"],
            created_at=data["created_at"],
            status=data["status"],
            expire_at=data.get("expire_at"),
        )


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
