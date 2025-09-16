from tap_tester.base_suite_tests.pagination_test import PaginationTest
from base import MS_GraphBaseTest

class MS_GraphPaginationTest(PaginationTest, MS_GraphBaseTest):
    """
    Ensure tap can replicate multiple pages of data for streams that use pagination.
    """

    @staticmethod
    def name():
        return "tap_tester_ms_graph_pagination_test"

    def streams_to_test(self):
        streams_to_exclude = {'groups', 'group_member', 'group_owner'}
        return self.expected_stream_names().difference(streams_to_exclude)
