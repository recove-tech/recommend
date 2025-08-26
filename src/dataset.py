from typing import List, Dict, Any, Iterable, Tuple
from collections import defaultdict
from dataclasses import dataclass, field

from uuid import uuid4

import pinecone

from .enums import VINTED_INDEX_NAME, RECOVE_INDEX_NAME
from .pinecone import fetch_vectors
from .models import PineconeUsage, PineconeUsageEntry


@dataclass
class BaseUserDataset:
    user_id: str
    point_ids: List[str]
    metadata_list: List[Dict[str, Any]]

    def __len__(self) -> int:
        return len(self.metadata_list)

    def __getitem__(self, index: int) -> Tuple[str, str, Dict[str, Any]]:
        point_id = self.point_ids[index]
        metadata = self.metadata_list[index]

        return self.user_id, point_id, metadata

    @classmethod
    def from_bigquery_rows(cls, **kwargs) -> "BaseUserDataset":
        pass


@dataclass
class TextUserDataset(BaseUserDataset):
    texts: List[str] = field(default_factory=list)

    def is_valid(self) -> bool:
        return len(self.texts) > 0

    @classmethod
    def from_bigquery_rows(
        cls, user_id: str, rows: Iterable, min_text_size: int
    ) -> "TextUserDataset":
        point_ids, metadata_list, texts = [], [], []

        for row in rows:
            if row.text and len(row.text.split()) > min_text_size:
                point_id = str(uuid4())
                point_ids.append(point_id)
                metadata_list.append(dict(row))
                texts.append(row.text)

        if point_ids:
            return cls(
                user_id=user_id,
                point_ids=point_ids,
                metadata_list=metadata_list,
                texts=texts,
            )


@dataclass
class VectorUserDataset(BaseUserDataset):
    embeddings: List[List[float]] = field(default_factory=list)
    usage: PineconeUsage = field(default_factory=PineconeUsage)

    def is_valid(self) -> bool:
        return len(self.embeddings) > 0

    @classmethod
    def from_bigquery_rows(
        cls,
        user_id: str,
        rows: Iterable,
        index_mapping: Dict[str, pinecone.Index],
        user_item_index: List[Tuple[str, str]] = [],
    ) -> "VectorUserDataset":
        metadata_list, embeddings = [], []
        usage = PineconeUsage()

        point_ids_dict = {
            VINTED_INDEX_NAME: defaultdict(list),
            RECOVE_INDEX_NAME: defaultdict(list),
        }

        for row in rows:
            point_id = row["point_id"]
            item_id = row["item_id"]
            namespace = row["category_type"]
            index_name = row["index_name"]

            if (user_id, item_id) in user_item_index:
                continue

            point_ids_dict[index_name][namespace].append((point_id, item_id))

        vectors, point_ids, item_ids = [], [], []

        for index_name in point_ids_dict:
            for namespace in point_ids_dict[index_name]:
                namespace_data = point_ids_dict[index_name][namespace]
                namespace_point_ids = [point_id for point_id, _ in namespace_data]
                namespace_item_ids = [item_id for _, item_id in namespace_data]

                namespace_vectors, read_units = fetch_vectors(
                    index=index_mapping[index_name],
                    namespace=namespace,
                    point_ids=namespace_point_ids,
                )

                vectors.extend(namespace_vectors)
                point_ids.extend(namespace_point_ids)
                item_ids.extend(namespace_item_ids)
                usage.add(PineconeUsageEntry(read_units=read_units))

        for vector, item_id in zip(vectors, item_ids):
            embedding = vector.values

            metadata = vector.metadata
            metadata["user_id"] = user_id
            metadata["item_id"] = item_id

            embeddings.append(embedding)
            metadata_list.append(metadata)

        return cls(
            user_id=user_id,
            point_ids=point_ids,
            metadata_list=metadata_list,
            embeddings=embeddings,
            usage=usage,
        )
