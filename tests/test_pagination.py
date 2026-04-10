from tap_tester.base_suite_tests.pagination_test import PaginationTest
from base import MS_GraphBaseTest

class MS_GraphPaginationTest(PaginationTest, MS_GraphBaseTest):
    """
    Ensure tap can replicate multiple pages of data for streams that use pagination.
    """

    @staticmethod
    def name():
        return "tap_tester_ms_graph_pagination_test"

    def get_properties(self, original: bool = True):
        props = super().get_properties(original)
        props["page_size"] = 1
        return props

    @classmethod
    def expected_metadata(cls):
        metadata = super().expected_metadata()
        for stream in metadata:
            metadata[stream][cls.API_LIMIT] = 1
        return metadata

    def streams_to_test(self):
        # Exclude streams with no records
        streams_to_exclude = {
            'audit_logs_signins', 'chats', 'conditional_access_policies',
            'drives', 'calendar_events', 'contacts', 'mail_messages',
            'chat_messages', 'drive_items', 'audit_logs_directory', 'teams', 'users',
            'channels', 'team_member', 'directory_role_member', 'directory_roles', 'applications'
        }
        return self.expected_stream_names().difference(streams_to_exclude)
