from typing import Dict, Iterator, List
from singer import get_logger
from tap_ms_graph.streams.abstracts import FullTableStream

LOGGER = get_logger()


class DirectoryRoles(FullTableStream):
    tap_stream_id = "directory_roles"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    data_key = "value"
    path = "directoryRoles"
