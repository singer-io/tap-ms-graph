from typing import Dict, Iterator, List
from singer import get_logger
from tap_ms_graph.streams.abstracts import FullTableStream

LOGGER = get_logger()


class GroupMember(FullTableStream):
    tap_stream_id = "group_member"
    key_properties = ["group_id", "id"]
    replication_method = "FULL_TABLE"
    data_key = "value"
    path = "groups/{group_id}/members"
    path = "groups"
