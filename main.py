import os, json
from pinecone import Pinecone
import src


BATCH_SIZE = None


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
            index=user_vectors_index, vectors=vectors, namespace=namespace
        ):
            return 0

        if not src.bigquery.upload(
            client=bq_client,
            dataset_id=src.enums.PROD_DATASET_ID,
            table_id=src.enums.USER_VECTOR_TABLE_ID,
            rows=bq_rows,
        ):
            return 0

        return src.supabase.upload(
            supabase_url=supabase_url,
            supabase_key=supabase_key,
            table_id=src.enums.USER_VECTOR_TABLE_ID,
            rows=supabase_rows,
        )

    except Exception as e:
        print(e)
        return 0


def main():
    secrets = json.loads(os.getenv("SECRETS_JSON"))

    global supabase_url, supabase_key
    supabase_url = secrets["SUPABASE_URL"]
    supabase_key = secrets["SUPABASE_SERVICE_ROLE_KEY"]

    global bq_client, user_vectors_index, items_index

    bq_client = src.bigquery.init_client(secrets["GCP_CREDENTIALS"])

    pc_client = Pinecone(api_key=secrets.get("PINECONE_API_KEY"))
    user_vectors_index = pc_client.Index(src.enums.USER_VECTORS_INDEX_NAME)
    items_index = pc_client.Index(src.enums.ITEMS_INDEX_NAME)

    loader = src.bigquery.load_items(client=bq_client)
    user_item_index = src.supabase.get_user_item_index(supabase_url, supabase_key)

    n, n_success, n_inserted = 0, 0, 0

    for user_id, group in loader:
        dataset = src.dataset.VectorUserDataset.from_bigquery_rows(
            user_id=user_id,
            rows=group,
            fetch_vectors_fn=src.pinecone.fetch_vectors,
            fetch_vectors_kwargs={"index": items_index},
            user_item_index=user_item_index,
        )

        if dataset:
            n_inserted_ = process_user_dataset(dataset)
            n_success += min(n_inserted_, 1)
            n_inserted += n_inserted_
            n += 1

        success_rate = n_success / n if n > 0 else 0

        print(
            f"User: {user_id} | "
            f"Inserted: {n_inserted_} | "
            f"Total users: {n} | "
            f"Total Inserted: {n_inserted} | "
            f"Success rate: {success_rate:.2f}"
        )


if __name__ == "__main__":
    main()
