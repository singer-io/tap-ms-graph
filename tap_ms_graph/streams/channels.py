from typing import Dict, Iterator, List
from singer import get_logger
from tap_ms_graph.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Channels(FullTableStream):
    tap_stream_id = "channels"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "value"
    path = "teams/{team_id}/channels"
    parent = "teams"
    http_method = "GET"


    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """Prepare URL endpoint for child streams."""
        return f"{self.client.base_url}/{self.path.format(team_id = parent_obj['id'])}"
        
    def update_params(self, **kwargs) -> None:
        # Add $top=999
        self.params.update({
            "$top": 999
        })
        self.params.update(kwargs)
