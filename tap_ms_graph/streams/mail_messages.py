from typing import Dict, Iterator, List
from singer import get_logger
from tap_ms_graph.streams.abstracts import FullTableStream

LOGGER = get_logger()


class MailMessages(FullTableStream):
    tap_stream_id = "mail_messages"
    key_properties = ["id", "user_id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "value"
    path = "users/{user_id}/messages"
    parent = "users"
