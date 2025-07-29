from typing import Dict, Iterator, List
from singer import get_logger
from tap_ms_graph.streams.abstracts import FullTableStream

LOGGER = get_logger()


class DriveItems(FullTableStream):
    tap_stream_id = "drive_items"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "value"
    path = "/users/{userId}/drives"
    parent = "users"
