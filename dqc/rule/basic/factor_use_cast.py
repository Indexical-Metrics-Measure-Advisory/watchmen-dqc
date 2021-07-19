from pandas import DataFrame

from dqc.common.constants import FACTOR_RULE
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.model.analysis.rule_result import RuleExecuteResult
from dqc.rule.utils.topic_utils import table_not_exist, data_is_empty, get_execute_factor_list, \
    init_factor_rule_result


def init():
    def check_use_cast(df_series, rule=None, factor=None):
        if df_series_is_str(df_series):
            result_flag = [df_series.str.isnumeric().all(), df_series.str.isdecimal().all()]
            return True in result_flag
        else:
            return None

    def df_series_is_str(df_series):
        return df_series.apply(type).eq(str).all()

    def factor_use_cast(df: DataFrame, topic, rule: MonitorRule):
        if table_not_exist(df) or data_is_empty(df):
            return None
        else:
            factor_rule_result_list = []
            execute_result = RuleExecuteResult()
            execute_result.ruleType = FACTOR_RULE
            factor_filtered = get_execute_factor_list(rule, topic)
            for factor in factor_filtered:
                factor_rule_result = init_factor_rule_result(rule, topic, factor)
                value = df[factor["name"].lower()]
                factor_rule_result.result = check_use_cast(value, rule, factor)
                factor_rule_result_list.append(factor_rule_result)
            execute_result.factorResult = factor_rule_result_list
            return execute_result

    return factor_use_cast
