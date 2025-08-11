from typing import Dict, Iterator, List
from singer import get_logger
from tap_ms_graph.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Teams(FullTableStream):
    tap_stream_id = "teams"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "value"
    params = {"$filter": "resourceProvisioningOptions/Any(x:x eq \u0027Team\u0027)"}
    path = "groups"
