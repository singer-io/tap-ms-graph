from typing import Dict, Iterator, List
from singer import get_logger
from tap_ms_graph.streams.abstracts import FullTableStream

LOGGER = get_logger()


class DirectoryRoleMember(FullTableStream):
    tap_stream_id = "directory_role_member"
    key_properties = ["role_id", "id"]
    replication_method = "FULL_TABLE"
    data_key = "value"
    path = "directoryRoles/{role_id}/members"
    path = "directory_roles"
