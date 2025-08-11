from typing import Dict, Iterator, List
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
