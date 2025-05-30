from typing import List, Dict, Tuple

import pinecone
from datetime import datetime
from .models import Vector, BigQueryRow, SupabaseRow


def prepare(
    point_ids: List[str], metadata_list: List[Dict], embeddings: List[List[float]]
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    item_index, vectors, bq_rows, supabase_rows = [], [], [], []

    for point_id, metadata, embedding in zip(point_ids, metadata_list, embeddings):
        item_id = metadata.get("vinted_id")

        if item_id and item_id not in item_index:
            bq_row = _create_bq_row(point_id, metadata)
            supabase_row = _create_supabase_row(point_id, metadata)
            vector = _create_vector(point_id, metadata, embedding)

            bq_rows.append(bq_row.__dict__)
            supabase_rows.append(supabase_row.__dict__)
            vectors.append(vector.__dict__)
            item_index.append(item_id)

    return vectors, bq_rows, supabase_rows


def upload(index: pinecone.Index, vectors: List[Dict], namespace: str) -> bool:
    if len(vectors) == 0:
        return False

    try:
        index.upsert(vectors=vectors, namespace=namespace)
        return True
    except:
        return False


def fetch_vectors(
    index: pinecone.Index, point_ids: List[str]
) -> List[pinecone.ScoredVector]:
    response = index.fetch(ids=point_ids)

    return response.vectors.values()


def _create_vector(point_id: str, metadata: Dict, embedding: List[float]) -> Vector:
    if metadata.get("item_id"):
        if metadata.get("created_at") and isinstance(metadata["created_at"], datetime):
            metadata["created_at"] = metadata["created_at"].isoformat()

        if metadata.get("updated_at") and isinstance(metadata["updated_at"], datetime):
            metadata["updated_at"] = metadata["updated_at"].isoformat()

        return Vector(id=point_id, values=embedding, metadata=metadata)


def _create_bq_row(point_id: str, metadata: Dict) -> BigQueryRow:
    return BigQueryRow(
        id=point_id,
        created_at=datetime.now().isoformat(),
        user_id=metadata.get("user_id"),
        item_id=metadata.get("item_id"),
    )


def _create_supabase_row(point_id: str, metadata: Dict) -> SupabaseRow:
    return SupabaseRow(
        user_id=metadata.get("user_id"),
        item_id=metadata.get("item_id"),
        point_id=point_id,
    )
