from typing import Dict, Iterator, List
from singer import get_logger
from tap_ms_graph.streams.abstracts import FullTableStream

LOGGER = get_logger()


class MailMessages(FullTableStream):
    tap_stream_id = "mail_messages"
    key_properties = ["id", "user_id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "value"
    path = "users/{user_id}/messages"
    parent = "users"


    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """Constructs the API endpoint URL for fetching mail messages for a given user."""
        if not parent_obj or 'id' not in parent_obj:
            raise ValueError("parent_obj must be provided with an 'id' key.")
        return f"{self.client.base_url}/{self.path.format(user_id = parent_obj['id'])}"

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """
        Modify the record before writing to the stream
        """
        if parent_record:
            record["user_id"] = parent_record.get("id")
        return record
