from dataclasses import dataclass, field
from typing import List, Dict, Any, Iterable, Tuple, Callable

from uuid import uuid4

from .utils import download_image_as_pil


@dataclass
class BaseUserDataset:
    user_id: str
    point_ids: List[str]
    metadata_list: List[Dict[str, Any]]

    def __len__(self) -> int:
        return len(self.point_ids)

    @classmethod
    def from_bigquery_rows(cls, **kwargs) -> "BaseUserDataset":
        pass


@dataclass
class ImageUserDataset(BaseUserDataset):
    images: List[Any] = field(default_factory=list)

    def is_valid(self) -> bool:
        return len(self.images) > 0

    @classmethod
    def from_bigquery_rows(
        cls,
        user_id: str,
        rows: Iterable,
        user_item_index: List[Tuple[str, str]],
    ) -> "ImageUserDataset":
        point_ids, metadata_list, images = [], [], []

        for row in rows:
            user_id = row["user_id"]
            item_id = row["item_id"]

            if (user_id, item_id) in user_item_index:
                continue

            image = download_image_as_pil(row["image_location"])

            if image:
                point_id = str(uuid4())
                point_ids.append(point_id)
                metadata_list.append(dict(row))
                images.append(image)

        if point_ids:
            return cls(
                user_id=user_id,
                point_ids=point_ids,
                metadata_list=metadata_list,
                images=images,
            )


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

    def is_valid(self) -> bool:
        return len(self.embeddings) > 0

    @classmethod
    def from_bigquery_rows(
        cls,
        user_id: str,
        rows: Iterable,
        fetch_vectors_fn: Callable,
        fetch_vectors_kwargs: Dict = {},
        user_item_index: List[Tuple[str, str]] = [],
    ) -> "VectorUserDataset":
        point_ids, metadata_list, embeddings = [], [], []

        for row in rows:
            point_id = row["point_id"]
            item_id = row["item_id"]

            if (user_id, item_id) in user_item_index:
                continue

            point_ids.append(point_id)

        fetch_vectors_kwargs["point_ids"] = point_ids
        vectors = fetch_vectors_fn(**fetch_vectors_kwargs)

        for vector in vectors:
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
        )
