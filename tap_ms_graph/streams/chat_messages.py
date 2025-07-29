from typing import Dict, Any
from singer import get_bookmark, get_logger
from tap_ms_graph.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class ChatMessages(IncrementalStream):
    tap_stream_id = "chat_messages"
    key_properties = ["chat_id", "id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["lastModifiedDateTime"]
    data_key = "value"
    path = "chats/{chat_id}/messages/delta"
    parent = "chats"
    bookmark_value = None

    def get_bookmark(self, state: Dict, key: Any = None) -> int:
        """
        Return initial bookmark value only for the child stream.
        """
        if not self.bookmark_value:        
            self.bookmark_value = super().get_bookmark(state, key)

        return self.bookmark_value
