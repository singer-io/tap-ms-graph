from typing import Dict, Iterator, List
from singer import get_logger
from tap_ms_graph.streams.abstracts import FullTableStream

LOGGER = get_logger()


class AuditLogsDirectoryaudits(FullTableStream):
    tap_stream_id = "audit_logs_directoryAudits"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "value"
    path = "auditLogs/directoryAudits"
    http_method = "GET"
