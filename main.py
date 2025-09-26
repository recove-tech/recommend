from typing import Optional, Tuple, Dict, List, Iterable

from google.cloud import bigquery
from collections import groupby

import src


BATCH_SIZE = None
SECRETS_PATH = "secrets.json"
DISPLAY_EVERY = 50


def reset_subscription_table() -> bool:
    subscriptions = src.supabase.get_subscriptions(
        session.supabase_url, session.supabase_key
    )

    query = src.bigquery.make_reset_subscription_table_query()
    success, result = src.bigquery.run_query(session.bq_client, query)

    if success:
        return src.bigquery.upload(
            client=session.bq_client,
            dataset_id=src.enums.BACKUP_DATASET_ID,
            table_id=src.enums.SUBSCRIPTION_TABLE_ID,
            rows=subscriptions,
        )

    return False


def get_items_dataloader(
    n_users: Optional[int] = None,
    only_new: bool = True,
) -> Tuple[Dict[str, List[bigquery.table.Row]], int]:
    query = src.bigquery.query_user_items(n_users, only_new)
    result = session.bq_client.query(query).result()

    if result.total_rows == 0:
        return [], 0

    loader = {}

    iterator = groupby(
        sorted(result, key=lambda x: x["user_id"]), key=lambda x: x["user_id"]
    )

    for user_id, group in iterator:
        loader[user_id] = list(group)

    return loader, result.total_rows


def process_user_dataset(dataset: src.dataset.VectorUserDataset) -> int:
    try:
        if not dataset.is_valid():
            return 0

        namespace = dataset.user_id

        vectors, bq_rows, supabase_rows = src.pinecone.prepare(
            point_ids=dataset.point_ids,
            metadata_list=dataset.metadata_list,
            embeddings=dataset.embeddings,
        )

        if not src.pinecone.upload(
            index=session.user_index, vectors=vectors, namespace=namespace
        ):
            return 0

        if not src.bigquery.upload(
            client=session.bq_client,
            dataset_id=session.bq_dataset_id,
            table_id=src.enums.USER_VECTOR_TABLE_ID,
            rows=bq_rows,
        ):
            return 0

        return src.supabase.upload(
            supabase_url=session.supabase_url,
            supabase_key=session.supabase_key,
            table_id=src.enums.USER_VECTOR_TABLE_ID,
            rows=supabase_rows,
        )

    except Exception as e:
        print(e)
        return 0


def process_loader(
    loader: Iterable, total_rows: int, only_new: bool, read_units: int = 0
) -> int:
    n, n_success, n_inserted = 0, 0, 0
    print(f"loader: {total_rows} | only_new: {only_new}")

    for user_id in loader:
        rows = loader[user_id]
        n_inserted_ = 0

        dataset = src.dataset.VectorUserDataset.from_bigquery_rows(
            user_id=user_id,
            rows=rows,
            index_mapping=session.index_mapping,
        )

        read_units += dataset.usage.read_units

        if len(dataset) > 0:
            n_inserted_ = process_user_dataset(dataset)
            n_success += min(n_inserted_, 1)
            n_inserted += n_inserted_
            n += 1

        success_rate = n_success / n if n > 0 else 0

        if not dataset.usage.is_empty:
            success = src.bigquery.upload(
                client=session.bq_client,
                dataset_id=src.enums.BACKUP_DATASET_ID,
                table_id=src.enums.PINECONE_USAGE_TABLE_ID,
                rows=[dataset.usage.to_dict()],
            )

        if n_inserted_:
            print(
                f"User: {user_id} | "
                f"Inserted: {n_inserted_} | "
                f"Total users: {n} | "
                f"Total Inserted: {n_inserted} | "
                f"Success rate: {success_rate:.2f} | "
                f"Uploaded usage: {success} | "
                f"Costs: {read_units * src.enums.PINECONE_PER_READ_UNIT_COST:.3f}$"
            )

    return n_inserted, read_units


def main() -> None:
    global session
    secrets = src.utils.load_json(SECRETS_PATH)
    session = src.session.Session(secrets=secrets)

    if not reset_subscription_table():
        raise Exception("Failed to reset subscription table")

    for only_new in [True, False]:
        loader, total_rows = get_items_dataloader(only_new=only_new)
        n_inserted, read_units = process_loader(loader, total_rows, only_new)
        print(f"{only_new=} | {n_inserted=} | {read_units=}")


if __name__ == "__main__":
    main()
