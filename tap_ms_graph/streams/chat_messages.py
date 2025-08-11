from typing import Dict, Iterator, List
from singer import get_logger
from tap_ms_graph.streams.abstracts import FullTableStream

LOGGER = get_logger()


class ChatMessages(FullTableStream):
    tap_stream_id = "chat_messages"
    key_properties = ["id", "chat_id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "value"
    path = "chats/{chat_id}/messages"
    parent = "chats"
