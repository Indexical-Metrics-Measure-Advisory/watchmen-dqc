import unittest

from pandas_profiling import ProfileReport

from dqc.rule.basic import raw_match_structure, factor_match_type
from dqc.sdk.admin.admin_sdk import load_all_topic_list
from dqc.service.analysis.analysis_service import load_global_rule_list
from dqc.service.common.site_service import load_site_json
from dqc.service.query.index import query_topic_data_by_datetime


class MyTestCase(unittest.TestCase):

    def __find_factor_match_type(self):
        rule_list = load_global_rule_list()
        for rule in rule_list:
            if rule.code == "raw-match-structure":
                return rule

    def test_raw_match_structure(self):

        rule = self.__find_factor_match_type()

        func = raw_match_structure.init()
        site: dict = load_site_json()
        print(site)
        topic_list = load_all_topic_list(site["local"])
        # print(topic_list)
        filtered = filter(lambda topic: topic["type"] != "raw" and topic["kind"] == "business" and topic[
            "name"] == "gimo_policy_change", topic_list)

        for topic in filtered:
            data_frame = query_topic_data_by_datetime("topic_gimo_policy_change", None, None)
            # data_frame = __get_topic_data(, None, None)
            print(func(data_frame, topic, rule))

        # profile = ProfileReport(data_frame, title="Pandas Profiling Report")
        #
        # profile.to_file("your_report.html")

        return list(filtered)


if __name__ == '__main__':
    unittest.main()
