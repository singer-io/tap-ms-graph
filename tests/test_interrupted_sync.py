
from base import MS_GraphBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class MS_GraphInterruptedSyncTest(MS_GraphBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    @staticmethod
    def name():
        return "tap_tester_ms_graph_interrupted_sync_test"

    def streams_to_test(self):
        return self.expected_stream_names()


    def manipulate_state(self):
        return {
            "currently_syncing": "prospects",
            "bookmarks": {
                "users": { "updatedDateTime" : "2020-01-01T00:00:00Z"},
                "groups": { "updatedDateTime" : "2020-01-01T00:00:00Z"},
        }
    }