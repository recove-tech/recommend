from typing import Dict, Literal
from dataclasses import dataclass

from pinecone import Pinecone

from .bigquery import init_client
from .enums import *


@dataclass
class Session:
    secrets: Dict
    mode: Literal["default", "mobile"]
    item_index_name: str = ITEMS_INDEX_NAME

    def __post_init__(self):
        self.bq_dataset_id = (
            PROD_DATASET_ID if self.mode == "default" else BACKUP_DATASET_ID
        )

        self.supabase_url = self.secrets["SUPABASE_URL"]
        self.supabase_key = self.secrets["SUPABASE_SERVICE_ROLE_KEY"]

        self.bq_client = init_client(self.secrets["GCP_CREDENTIALS"])

        self.pc_client = Pinecone(api_key=self.secrets.get("PINECONE_API_KEY"))

        self.item_index = self.pc_client.Index(self.item_index_name)

        user_index_name = (
            USER_VECTORS_INDEX_NAME if self.mode == "default" else USER_INDEX_NAME
        )
        self.user_index = self.pc_client.Index(user_index_name)
