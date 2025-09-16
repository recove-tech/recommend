from typing import Iterable

import src


BATCH_SIZE = None
SECRETS_PATH = "secrets/mobile.json"
DISPLAY_EVERY = 50
NUM_USERS = 2000
MIN_NUM_INSERTS = 1000


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


def main(loader: Iterable, total_rows: int, only_new: bool, read_units: int = 0) -> int:
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


if __name__ == "__main__":
    global session
    secrets = src.utils.load_json(SECRETS_PATH)
    session = src.session.Session(secrets=secrets)

    kwargs = {
        "client": session.bq_client,
        "dataset_id": session.bq_dataset_id,
        "only_new": True,
    }

    loader, total_rows = src.bigquery.get_items_dataloader(**kwargs)
    n_inserted, read_units = main(loader, total_rows, True)

    if n_inserted < MIN_NUM_INSERTS:
        print(
            f"Only {n_inserted} insertions with only_new=True, retrying with only_new=False"
        )

        kwargs["only_new"] = False
        kwargs["n_users"] = NUM_USERS
        loader, total_rows = src.bigquery.get_items_dataloader(**kwargs)

        main(loader, total_rows, False, read_units)
