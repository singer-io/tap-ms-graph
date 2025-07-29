from typing import Dict, Iterator, List
from singer import get_logger
from tap_ms_graph.streams.abstracts import FullTableStream

LOGGER = get_logger()


class DirectoryRoleMember(FullTableStream):
    tap_stream_id = "directory_role_member"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "value"
    path = "directoryRoles/{role_id}/members"
    parent = "directory_roles"
