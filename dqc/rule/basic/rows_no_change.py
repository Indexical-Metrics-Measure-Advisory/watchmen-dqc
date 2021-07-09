from pandas import DataFrame

from dqc.common.constants import TOPIC_RULE
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.rule.utils.topic_utils import data_is_empty, table_not_exist, init_topic_rule_result


def init():
    def rows_no_change(df: DataFrame, topic,rule:MonitorRule):

        ## TODO
        topic_rule_result = init_topic_rule_result(rule, topic)
        if table_not_exist(df) or data_is_empty(df):
            return TOPIC_RULE, None

        statistical_interval = rule.params["statisticalInterval"]
        coverage_rate = rule.params["coverageRate"]




        ## get topic data
        ## get data range by statisticalInterval




        return TOPIC_RULE, topic_rule_result
    return rows_no_change
