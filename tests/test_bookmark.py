from base import MS_GraphBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class MS_GraphBookMarkTest(BookmarkTest, MS_GraphBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {
            # "users": { "updatedDateTime" : "2020-01-01T00:00:00Z"},
            # "groups": { "updatedDateTime" : "2020-01-01T00:00:00Z"},
        }
    }
    @staticmethod
    def name():
        return "tap_tester_ms_graph_bookmark_test"

    def streams_to_test(self):
        streams_to_exclude = {'groups', 'group_member', 'group_owner'}
        return self.expected_stream_names().difference(streams_to_exclude)
