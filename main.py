from typing import Literal

import os
import src


BATCH_SIZE = None
SECRETS_DIR = "secrets"
DISPLAY_EVERY = 50


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


def main(mode: Literal["default", "mobile"] = "mobile"):
    global session
    secrets = src.utils.load_json(os.path.join(SECRETS_DIR, f"{mode}.json"))
    session = src.session.Session(secrets=secrets)

    loader, total_rows = src.bigquery.load_items(
        client=session.bq_client, dataset_id=session.bq_dataset_id
    )

    print(f"loader: {total_rows}")
    n, n_success, n_inserted = 0, 0, 0

    for user_id, group in loader:
        n_inserted_ = 0

        dataset = src.dataset.VectorUserDataset.from_bigquery_rows(
            user_id=user_id,
            rows=group,
            index_mapping=session.index_mapping,
        )

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
                f"Uploaded usage: {success}"
            )


if __name__ == "__main__":
    main()
