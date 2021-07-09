from pandas import DataFrame

from dqc.common.constants import TOPIC_RULE
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.rule.utils.topic_utils import data_is_empty, table_not_exist, init_topic_rule_result


def init():
    def rows_count_and_another(df: DataFrame, topic,rule:MonitorRule):
        topic_rule_result = init_topic_rule_result(rule, topic)
        if table_not_exist(df) or data_is_empty(df):
            return TOPIC_RULE, None

        statisticalInterval = rule.params["statisticalInterval"]

        topic_id = rule.params["topicId"]

        ## get topic data
        ## get data range by statisticalInterval








        return TOPIC_RULE, topic_rule_result
    return rows_count_and_another
