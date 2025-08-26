from typing import Dict, Iterable, Optional, Literal, Tuple
from itertools import groupby

from google.cloud import bigquery
from google.oauth2 import service_account

from .enums import *
from .models import InteractionType


def init_client(credentials_dict: Dict) -> bigquery.Client:
    credentials_dict["private_key"] = credentials_dict["private_key"].replace(
        "\\n", "\n"
    )

    credentials = service_account.Credentials.from_service_account_info(
        credentials_dict
    )

    return bigquery.Client(
        credentials=credentials, project=credentials_dict["project_id"]
    )


def upload(client: bigquery.Client, dataset_id: str, table_id: str, rows: Dict) -> bool:
    try:
        errors = client.insert_rows_json(
            table=f"{dataset_id}.{table_id}", json_rows=rows
        )

        return len(errors) == 0

    except:
        return False


def load_items(
    client: bigquery.Client,
    dataset_id: Literal[PROD_DATASET_ID, BACKUP_DATASET_ID],
    n: Optional[int] = None,
    index: Optional[int] = None,
) -> Tuple[Iterable, int]:
    query = _query_user_items(dataset_id, n, index)
    result = client.query(query).result()

    if result.total_rows == 0:
        return []

    loader = groupby(list(result), key=lambda x: x["user_id"])

    return loader, result.total_rows


def load_queries(
    client: bigquery.Client,
    from_recommend: bool = False,
    n: Optional[int] = None,
    index: Optional[int] = None,
) -> Iterable:
    query = _query_user_queries(from_recommend, n, index)
    result = client.query(query).result()

    if result.total_rows == 0:
        return []

    return groupby(list(result), key=lambda x: x["user_id"])


def _query_user_items(
    dataset_id: Literal[PROD_DATASET_ID, BACKUP_DATASET_ID],
    n: Optional[int] = None,
    index: Optional[int] = None,
) -> str:
    query = f"""
    WITH 
    UserItems AS (
        SELECT DISTINCT user_id, item_id, point_id, '{InteractionType.CLICK_OUT.value}' AS interaction_type, created_at
        FROM `{PROJECT_ID}.{dataset_id}.{CLICK_OUT_TABLE_ID}`
        WHERE point_id IS NOT NULL
        UNION ALL
        SELECT DISTINCT user_id, item_id, point_id, '{InteractionType.SAVED.value}' AS interaction_type, created_at
        FROM `{PROJECT_ID}.{dataset_id}.{SAVED_TABLE_ID}`
        WHERE point_id IS NOT NULL
    )
    , UserRecords AS (
        SELECT 
        ui.*,
        COALESCE(vinted.category_type, recove.category_type) AS category_type, 
        (
        CASE WHEN vinted.id IS NOT NULL 
        THEN "vinted" 
        ELSE CASE WHEN recove.id IS NOT NULL THEN "recove" ELSE NULL END
        END
        ) AS index_name
        FROM UserItems AS ui
        LEFT JOIN `{PROJECT_ID}.{VINTED_DATASET_ID}.{ITEM_METADATA_TABLE_ID}` AS vinted ON ui.item_id = vinted.id
        LEFT JOIN `{PROJECT_ID}.{RECOVE_DATASET_ID}.{ITEM_TABLE_ID}` AS recove ON ui.item_id = recove.id
    )
    , Data AS (
        SELECT ur.*,
        ROW_NUMBER() OVER (PARTITION BY CONCAT(ur.user_id, ur.item_id) ORDER BY ur.interaction_type) as row_num
        FROM UserRecords AS ur
        LEFT JOIN `{PROJECT_ID}.{dataset_id}.{USER_VECTOR_TABLE_ID}` AS uv
        ON CONCAT(uv.user_id, uv.item_id) = CONCAT(ur.user_id, ur.item_id)
        WHERE CONCAT(uv.user_id, uv.item_id) IS NULL AND ur.index_name IS NOT NULL AND ur.category_type IS NOT NULL
    )
    SELECT * EXCEPT(row_num)
    FROM Data
    WHERE row_num = 1
    ORDER BY created_at DESC
    """

    if n:
        query += f" LIMIT {n}"

        if index:
            query += f" OFFSET {index * n}"

    return query


def _query_user_queries(
    from_recommend: bool, n: Optional[int] = None, index: Optional[int] = None
) -> str:
    if from_recommend:
        table_id = f"{RECOMMEND_DATASET_ID}.{QUERY_TABLE_ID}"
        pin_field_id = "pin_id"
    else:
        table_id = f"{PROD_DATASET_ID}.{QUERIES_TABLE_ID}"
        pin_field_id = "image_url"

    query = f"""
    WITH 
    queries AS (
    SELECT 
    q.*,
    ROW_NUMBER() OVER (PARTITION BY CONCAT(q.user_id, q.{pin_field_id}, q.text) ORDER BY q.created_at DESC) as row_num
    FROM `{PROJECT_ID}.{table_id}` AS q
    LEFT JOIN `{PROJECT_ID}.{PROD_DATASET_ID}.{USER_VECTOR_TABLE_ID}` v
    ON CONCAT(v.user_id, v.query_id) = CONCAT(q.user_id, q.id)
    WHERE CONCAT(v.user_id, v.query_id) IS NULL AND v.item_id IS NULL
    )
    SELECT * EXCEPT(row_num, id), id AS query_id, NULL AS item_id
    FROM queries
    WHERE row_num = 1
    """

    if n:
        query += f"LIMIT {n}"

        if index:
            query += f"OFFSET {index * n}"

    return query
