from typing import Dict
from dataclasses import dataclass

from pinecone import Pinecone, Index

from .bigquery import init_client
from .enums import *


@dataclass
class Session:
    secrets: Dict

    def __post_init__(self):
        self.bq_dataset_id = BACKUP_DATASET_ID

        self.supabase_url = self.secrets["SUPABASE_URL"]
        self.supabase_key = self.secrets["SUPABASE_SERVICE_ROLE_KEY"]

        self.bq_client = init_client(self.secrets["GCP_CREDENTIALS"])

        self.pc_client = Pinecone(api_key=self.secrets.get("PINECONE_API_KEY"))
        self.vinted_index = self.pc_client.Index(VINTED_INDEX_NAME)
        self.user_index = self.pc_client.Index(USER_INDEX_NAME)

    @property
    def index_mapping(self) -> Dict[str, Index]:
        return {
            VINTED_INDEX_NAME: self.vinted_index,
            USER_INDEX_NAME: self.user_index,
        }
