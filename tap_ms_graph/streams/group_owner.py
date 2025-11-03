from typing import Dict, Iterator, List
from singer import get_logger
from tap_ms_graph.streams.abstracts import FullTableStream

LOGGER = get_logger()


class GroupOwner(FullTableStream):
    tap_stream_id = "group_owner"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "value"
    path = "groups/{group_id}/owners"
    parent = "groups"


    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """Constructs the API endpoint URL for fetching group owner for a given group."""
        if not parent_obj or 'id' not in parent_obj:
            raise ValueError("parent_obj must be provided with an 'id' key.")
        return f"{self.client.base_url}/{self.path.format(group_id = parent_obj['id'])}"
