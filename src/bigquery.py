from typing import Dict, List, Optional, Literal, Tuple
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


def get_items_dataloader(
    client: bigquery.Client,
    dataset_id: Literal[PROD_DATASET_ID, BACKUP_DATASET_ID],
    n_users: Optional[int] = None,
    only_new: bool = True,
    user_ids: Optional[List[str]] = None,
) -> Tuple[Dict[str, List[bigquery.table.Row]], int]:
    query = _query_user_items(dataset_id, n_users, only_new, user_ids)
    result = client.query(query).result()

    if result.total_rows == 0:
        return [], 0

    loader = {}

    iterator = groupby(
        sorted(result, key=lambda x: x["user_id"]), key=lambda x: x["user_id"]
    )

    for user_id, group in iterator:
        loader[user_id] = list(group)

    return loader, result.total_rows


def _query_user_items(
    dataset_id: Literal[PROD_DATASET_ID, BACKUP_DATASET_ID],
    n_users: Optional[int] = None,
    only_new: bool = False,
    user_ids: Optional[List[str]] = None,
) -> str:
    if user_ids:
        user_ids_str = [f"'{user_id}'" for user_id in user_ids]
        user_id_condition = f"AND user_id IN ({', '.join(user_ids_str)})"
    else:
        user_id_condition = ""

    query = f"""
WITH 
UserItems AS (
    SELECT DISTINCT user_id, item_id, point_id, '{InteractionType.CLICK_OUT.value}' AS interaction_type, created_at
    FROM `{PROJECT_ID}.{dataset_id}.{CLICK_OUT_TABLE_ID}`
    WHERE point_id IS NOT NULL {user_id_condition}
    UNION ALL
    SELECT DISTINCT user_id, item_id, point_id, '{InteractionType.SAVED.value}' AS interaction_type, created_at
    FROM `{PROJECT_ID}.{dataset_id}.{SAVED_TABLE_ID}`
    WHERE point_id IS NOT NULL {user_id_condition})
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
    LEFT JOIN `{PROJECT_ID}.{RECOVE_DATASET_ID}.{ITEM_TABLE_ID}` AS recove ON ui.item_id = recove.id)
, Data AS (
    SELECT ur.*,
    ROW_NUMBER() OVER (PARTITION BY CONCAT(ur.user_id, ur.item_id) ORDER BY ur.interaction_type) as row_num
    FROM UserRecords AS ur
    LEFT JOIN `{PROJECT_ID}.{dataset_id}.{USER_VECTOR_TABLE_ID}` AS uv
    ON CONCAT(uv.user_id, uv.item_id) = CONCAT(ur.user_id, ur.item_id)
    """

    if only_new:
        query += f"""
    WHERE NOT EXISTS (
    SELECT 1 
    FROM `{PROJECT_ID}.{dataset_id}.{USER_VECTOR_TABLE_ID}` AS existing_users 
    WHERE existing_users.user_id = ur.user_id)
        """
    else:
        query += f"""
    WHERE CONCAT(uv.user_id, uv.item_id) IS NULL
        """

    query += f"""
    AND ur.index_name IS NOT NULL AND ur.category_type IS NOT NULL)

, Users AS (
    SELECT DISTINCT user_id
    FROM Data
    WHERE row_num = 1
"""

    if n_users:
        query += f"""
    ORDER BY RAND()
    LIMIT {n_users})
        """
    else:
        query += ")"

    query += f"""
SELECT d.* EXCEPT(row_num)
FROM Data d
INNER JOIN Users u USING(user_id)
WHERE d.row_num = 1
"""

    return query
