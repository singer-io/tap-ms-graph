from typing import Dict, Iterator, List
from singer import get_logger
from tap_ms_graph.streams.abstracts import FullTableStream

LOGGER = get_logger()


class ConditionalAccessPolicies(FullTableStream):
    tap_stream_id = "conditional_access_policies"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "value"
    path = "identity/conditionalAccess/policies"
    http_method = "GET"
