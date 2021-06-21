from pandas import DataFrame

from dqc.common.constants import TOPIC_RULE
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.rule.utils.topic_utils import data_is_empty, table_not_exist, init_topic_rule_result


def init():
    def rows_not_exists(df: DataFrame, topic,rule:MonitorRule):
        topic_rule_result = init_topic_rule_result(rule, topic)
        if table_not_exist(df) or data_is_empty(df):
            topic_rule_result.result = True
        else:
            topic_rule_result.result = False

        return TOPIC_RULE, topic_rule_result
    return rows_not_exists
