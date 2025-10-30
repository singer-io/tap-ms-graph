from typing import Dict, Iterator, List
from singer import get_logger
from tap_ms_graph.streams.abstracts import FullTableStream

LOGGER = get_logger()


class DirectoryRoleTemplates(FullTableStream):
    tap_stream_id = "directory_role_templates"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "value"
    path = "directoryRoleTemplates"


    def update_params(self, **kwargs) -> None:
        """
        Update params for the stream
        """
        self.params.update(kwargs)