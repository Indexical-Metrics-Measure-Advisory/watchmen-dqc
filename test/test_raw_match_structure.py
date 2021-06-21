import unittest

from dqc.rule import raw_match_structure
from dqc.sdk.admin.admin_sdk import load_all_topic_list
from dqc.service.common.site_service import load_site_json
from dqc.service.query.index import query_topic_data_by_datetime


class MyTestCase(unittest.TestCase):

    def test_raw_match_structure(self):
        func = raw_match_structure.init()
        site: dict = load_site_json()
        topic_list = load_all_topic_list(site["local"])
        # print(topic_list)
        filtered = filter(lambda topic: topic["type"] != "raw" and topic["kind"] == "business" and topic[
            "name"] == "test_distinct_data", topic_list)

        for topic in filtered:
            data_frame = query_topic_data_by_datetime("topic_test_distinct_data", None, None)
            # data_frame = __get_topic_data(, None, None)
            print(func(data_frame, topic))

        return list(filtered)


if __name__ == '__main__':
    unittest.main()
