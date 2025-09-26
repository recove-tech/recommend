from typing import Optional

from ..models import InteractionType
from .enums import *


def make_reset_subscription_table_query() -> bool:
    return f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{BACKUP_DATASET_ID}.{SUBSCRIPTION_TABLE_ID}` AS
SELECT * FROM `{PROJECT_ID}.{BACKUP_DATASET_ID}.{SUBSCRIPTION_TABLE_ID}`
LIMIT 0
    """


def query_user_items(
    n_users: Optional[int] = None,
    only_new: bool = False,
) -> str:
    query = f"""
WITH 
UserItems AS (
    SELECT DISTINCT click_out.*, '{InteractionType.CLICK_OUT.value}' AS interaction_type
    FROM `{PROJECT_ID}.{BACKUP_DATASET_ID}.{CLICK_OUT_TABLE_ID}` AS click_out
    INNER JOIN `{PROJECT_ID}.{BACKUP_DATASET_ID}.{SUBSCRIPTION_TABLE_ID}` AS subscription USING (user_id)
    WHERE point_id IS NOT NULL
    UNION ALL
    SELECT DISTINCT saved.*, '{InteractionType.SAVED.value}' AS interaction_type
    FROM `{PROJECT_ID}.{BACKUP_DATASET_ID}.{SAVED_TABLE_ID}` AS saved
    INNER JOIN `{PROJECT_ID}.{BACKUP_DATASET_ID}.{SUBSCRIPTION_TABLE_ID}` AS subscription USING (user_id)
    WHERE point_id IS NOT NULL
)
, UserRecords AS (
    SELECT 
    ui.*,
    pinecone.point_id, 
    pinecone.namespace, 
    FROM UserItems AS ui
    LEFT JOIN `{PROJECT_ID}.{VINTED_DATASET_ID}.{PINECONE_TABLE_ID}` AS pinecone USING (item_id))
, Data AS (
    SELECT ur.*,
    ROW_NUMBER() OVER (PARTITION BY CONCAT(ur.user_id, ur.item_id) ORDER BY ur.interaction_type) as row_num
    FROM UserRecords AS ur
    LEFT JOIN `{PROJECT_ID}.{BACKUP_DATASET_ID}.{USER_VECTOR_TABLE_ID}` AS uv
    ON CONCAT(uv.user_id, uv.item_id) = CONCAT(ur.user_id, ur.item_id)
    """

    if only_new:
        query += f"""
    WHERE NOT EXISTS (
    SELECT 1 
    FROM `{PROJECT_ID}.{BACKUP_DATASET_ID}.{USER_VECTOR_TABLE_ID}` AS existing_users 
    WHERE existing_users.user_id = ur.user_id)
        """
    else:
        query += f"""
    WHERE CONCAT(uv.user_id, uv.item_id) IS NULL
        """

    query += f"""
    AND ur.index_name IS NOT NULL AND ur.namespace IS NOT NULL)

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
