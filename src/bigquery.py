from typing import Dict, Iterable, Optional
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
    client: bigquery.Client, n: Optional[int] = None, index: Optional[int] = None
) -> Iterable:
    query = _query_user_items(n, index)
    result = client.query(query).result()

    if result.total_rows == 0:
        return []

    return groupby(list(result), key=lambda x: x["user_id"])


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


def _query_user_items(n: Optional[int] = None, index: Optional[int] = None) -> str:
    query = f"""
    WITH 
        user_items AS (
        SELECT DISTINCT user_id, item_id, point_id, '{InteractionType.CLICK_OUT.value}' AS interaction_type
        FROM `{PROJECT_ID}.{PROD_DATASET_ID}.{CLICK_OUT_TABLE_ID}`
        WHERE point_id IS NOT NULL
        UNION ALL
        SELECT DISTINCT user_id, item_id, point_id, '{InteractionType.SAVED.value}' AS interaction_type
        FROM `{PROJECT_ID}.{PROD_DATASET_ID}.{SAVED_TABLE_ID}`
        WHERE point_id IS NOT NULL
        )
        , numbered_vectors AS (
        SELECT ui.*,
        ROW_NUMBER() OVER (PARTITION BY CONCAT(ui.user_id, ui.item_id) ORDER BY ui.interaction_type) as row_num
        FROM user_items ui
        LEFT JOIN `{PROJECT_ID}.{PROD_DATASET_ID}.{USER_VECTOR_TABLE_ID}` AS uv
        ON CONCAT(uv.user_id, uv.item_id) = CONCAT(ui.user_id, ui.item_id)
        WHERE CONCAT(uv.user_id, uv.item_id) IS NULL
        )
    SELECT * EXCEPT(row_num)
    FROM numbered_vectors
    WHERE row_num = 1;
    """

    if n:
        query += f"LIMIT {n}"

        if index:
            query += f"OFFSET {index * n}"

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
