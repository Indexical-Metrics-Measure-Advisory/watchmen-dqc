from pandas import DataFrame

from dqc.common.constants import FACTOR_RULE
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.model.analysis.rule_result import RuleExecuteResult
from dqc.rule.utils.topic_utils import table_not_exist, data_is_empty, get_execute_factor_list, \
    init_factor_rule_result


def init():
    def check_empty_coverage(df_series, rule=None, factor=None):
        null_count = df_series.isnull().sum()
        count = df_series.count()
        coverage_rate = rule.params.coverageRate
        if coverage_rate is None:
            raise ValueError("coverage rate is None")
        else:
            return null_count / count <= coverage_rate / 100

    def factor_empty_over_coverage(df: DataFrame, topic, rule: MonitorRule):
        if table_not_exist(df) or data_is_empty(df):
            return None
        else:
            factor_rule_result_list = []
            factor_filtered = get_execute_factor_list(rule, topic)
            execute_result = RuleExecuteResult()
            execute_result.ruleType = FACTOR_RULE

            for factor in factor_filtered:
                factor_rule_result = init_factor_rule_result(rule, topic, factor)
                value = df[factor["name"].lower()]
                factor_rule_result.result = not check_empty_coverage(value, rule, factor)
                factor_rule_result_list.append(factor_rule_result)

            execute_result.factorResult = factor_rule_result_list
            return execute_result

    return factor_empty_over_coverage
