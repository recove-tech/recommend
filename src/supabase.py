from typing import List, Dict, Tuple
from datetime import datetime
from supabase import create_client, Client

from .models import Subscription
from .enums import USER_VECTOR_TABLE_ID, GET_ACTIVE_SUBSCRIPTIONS_RPC_NAME


def init_supabase_client(url: str, key: str) -> Client:
    return create_client(supabase_url=url, supabase_key=key)


def upload(
    supabase_url: str,
    supabase_key: str,
    table_id: str,
    rows: List[Dict],
) -> int:
    supabase_client = init_supabase_client(supabase_url, supabase_key)

    try:
        response = supabase_client.table(table_id).upsert(rows).execute()
        return len(response.data)

    except Exception as e:
        num_inserted = 0

        for row in rows:
            try:
                supabase_client.table(table_id).insert(row).execute()
                num_inserted += 1
            except Exception as e:
                pass

        return num_inserted


def get_user_item_index(supabase_url: str, supabase_key: str) -> List[Tuple[str, str]]:
    supabase_client = init_supabase_client(supabase_url, supabase_key)

    try:
        response = (
            supabase_client.table(USER_VECTOR_TABLE_ID)
            .select("user_id, item_id")
            .execute()
        )

        distinct_pairs = set()
        for row in response.data:
            distinct_pairs.add((row["user_id"], row["item_id"]))

        return list(distinct_pairs)

    except Exception as e:
        print(e)
        return []


def get_subscriptions(supabase_url: str, supabase_key: str) -> List[Dict]:
    supabase_client = init_supabase_client(supabase_url, supabase_key)

    try:
        response = supabase_client.rpc(GET_ACTIVE_SUBSCRIPTIONS_RPC_NAME).execute()
        entries = []

        for data in response.data:
            try:
                subscription = Subscription.from_supabase(data)
                entries.append(subscription.to_dict())
            except Exception as e:
                pass

        return entries

    except Exception as e:
        print(e)
        return []
