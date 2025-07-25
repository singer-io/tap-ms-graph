from typing import Dict, Any
from singer import get_bookmark, get_logger
from tap_ms_graph.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Groups(IncrementalStream):
    tap_stream_id = "groups"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updatedDateTime"]
    data_key = "value"
    path = "groups/delta"
