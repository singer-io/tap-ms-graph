from typing import Dict, Iterator, List
from singer import get_logger
from tap_ms_graph.streams.abstracts import FullTableStream

LOGGER = get_logger()


class GroupMember(FullTableStream):
    tap_stream_id = "group_member"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "value"
    path = "groups/{group_id}/members"
    parent = "groups"
