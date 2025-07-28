from typing import Dict, Any
from singer import get_bookmark, get_logger
from tap_ms_graph.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Users(FullTableStream):
    tap_stream_id = "users"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "value"
    path = "users"
