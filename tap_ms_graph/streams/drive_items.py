from typing import Dict, Any
from singer import get_bookmark, get_logger
from tap_ms_graph.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class DriveItems(IncrementalStream):
    tap_stream_id = "drive_items"
    key_properties = ["drive_id", "id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModifiedDateTime"]
    data_key = "value"
    path = "drives/{drive_id}/items/delta"
    parent = "drives"
    bookmark_value = None

    def get_bookmark(self, state: Dict, key: Any = None) -> int:
        """
        Return initial bookmark value only for the child stream.
        """
        if not self.bookmark_value:        
            self.bookmark_value = super().get_bookmark(state, key)

        return self.bookmark_value
