from pandas import DataFrame

from dqc.common.constants import FACTOR_RULE
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.model.analysis.rule_result import RuleExecuteResult
from dqc.rule.utils.factor_utils import check_value_match_type, check_date_type, find_factor
from dqc.rule.utils.topic_utils import table_not_exist, data_is_empty, init_factor_rule_result


def init():
    def factor_mismatch_date_type(df: DataFrame, topic, rule: MonitorRule):
        if table_not_exist(df) or data_is_empty(df):
            return  None
        else:
            factor_rule_result_list = []
            factor_filtered = get_execute_factor_list(rule, topic)
            execute_result = RuleExecuteResult()
            execute_result.ruleType = FACTOR_RULE
            for factor in factor_filtered:
                factor_rule_result = init_factor_rule_result(rule, topic,factor)
                # print(factor)
                factor_type = factor["type"]
                value = df[factor["name"].lower()]
                factor_rule_result.result = not check_date_type(value, factor_type)
                factor_rule_result_list.append(factor_rule_result)
            execute_result.factorResult= factor_rule_result_list
            return execute_result

    def get_execute_factor_list(rule, topic):
        factor_list = topic["factors"]
        if rule.factorId is not None:
            factor_filtered = find_factor(factor_list, rule.factorId)
        else:
            factor_filtered = filter(
                lambda factor: factor["type"] in ["date", "datetime", "full-datetime", "date-of-birth"], factor_list)
        return factor_filtered

    return factor_mismatch_date_type
