from pandas import DataFrame

from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.rule.utils.topic_utils import data_is_empty, table_not_exist, init_topic_rule_result


def init():
    def rows_not_exists(df: DataFrame, topic, rule: MonitorRule):
        execute_result = init_topic_rule_result(rule, topic)
        if table_not_exist(df) or data_is_empty(df):
            execute_result.topicResult.result = True
        else:
            execute_result.topicResult.result = False
        return execute_result

    return rows_not_exists
