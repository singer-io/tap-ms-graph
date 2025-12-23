from typing import Dict
from singer import get_logger
from tap_ms_graph.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Channels(FullTableStream):
    tap_stream_id = "channels"
    key_properties = ["id", "team_id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "value"
    path = "teams/{team_id}/channels"
    parent = "teams"


    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """Constructs the API endpoint URL for fetching channels for a given team."""
        if not parent_obj or 'id' not in parent_obj:
            raise ValueError("parent_obj must be provided with an 'id' key.")
        return f"{self.client.base_url}/{self.path.format(team_id=parent_obj['id'])}"

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """
        Modify the record before writing to the stream
        """
        if parent_record:
            record["team_id"] = parent_record.get("id")
        return record
