from pandas import DataFrame

from dqc.common.constants import FACTOR_RULE
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.rule.utils.factor_utils import check_value_match_type
from dqc.rule.utils.topic_utils import table_not_exist, data_is_empty, init_factor_rule_result


def init():
    def factor_match_date_type(df: DataFrame, topic, rule: MonitorRule):
        if table_not_exist(df) or data_is_empty(df):
            return FACTOR_RULE, None
        else:
            factor_list = topic["factors"]

            factor_rule_result_list = []
            for factor in factor_list:
                factor_rule_result = init_factor_rule_result(rule, topic)
                # print(factor)
                factor_type = factor["type"]
                value = df[factor["name"].lower()]
                factor_rule_result.result = check_value_match_type(value, factor_type)
                factor_rule_result_list.append(factor_rule_result)

            return FACTOR_RULE, factor_rule_result_list

    return factor_match_date_type
