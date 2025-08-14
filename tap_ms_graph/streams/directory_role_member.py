from typing import Dict, Iterator, List
from singer import get_logger
from tap_ms_graph.streams.abstracts import FullTableStream

LOGGER = get_logger()


class DirectoryRoleMember(FullTableStream):
    tap_stream_id = "directory_role_member"
    key_properties = ["id", "role_id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "value"
    path = "directoryRoles/{role_id}/members"
    parent = "directory_roles"


    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """Constructs the API endpoint URL for fetching directory role member for a given role."""
        if not parent_obj or 'id' not in parent_obj:
            raise ValueError("parent_obj must be provided with an 'id' key.")
        return f"{self.client.base_url}/{self.path.format(role_id = parent_obj['id'])}"
