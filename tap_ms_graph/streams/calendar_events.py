from typing import Dict
from singer import get_logger
from tap_ms_graph.streams.abstracts import FullTableStream

LOGGER = get_logger()


class CalendarEvents(FullTableStream):
    tap_stream_id = "calendar_events"
    key_properties = ["id", "user_id"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    data_key = "value"
    path = "users/{user_id}/events"
    parent = "users"

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """Constructs the API endpoint URL for fetching calendar events for a given user."""
        if not parent_obj or 'id' not in parent_obj:
            raise ValueError("parent_obj must be provided with an 'id' key.")
        return f"{self.client.base_url}/{self.path.format(user_id = parent_obj['id'])}"
