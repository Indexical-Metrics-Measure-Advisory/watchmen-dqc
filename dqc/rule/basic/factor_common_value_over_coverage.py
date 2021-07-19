## print(df_series.mode().loc[0])


from pandas import DataFrame

from dqc.common.constants import FACTOR_RULE
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.model.analysis.rule_result import RuleExecuteResult
from dqc.rule.utils.topic_utils import table_not_exist, data_is_empty, get_execute_factor_list, \
    init_factor_rule_result


def init():
    def common_value_over_coverage(df_series, rule=None, factor=None):
        common_value = df_series.mode().loc[0]
        count_common_value = df_series.value_counts()[common_value]
        df_sum = df_series.sum()
        aggregation = rule.params.aggregation
        if aggregation is None:
            raise ValueError("aggregation is null")
        coverage_rate = rule.params.coverageRate
        if coverage_rate is None:
            raise ValueError("aggregation is null")
        df_count = df_series.count()
        aggregation_limit = df_count * aggregation / 100
        df_count_list = df_series.value_counts()
        loop_data = df_count_list.to_list()
        match_data_list = get_aggregation_fit_data(aggregation_limit, loop_data)
        # print(df_count_list.where(df_count_list in match_data_list.keys()))
        match_data_sum = 0
        for match_data in match_data_list:
            value = df_count_list.index[match_data["index"]]
            match_data_sum = match_data_sum + (value * match_data["count_value"])
        # print(match_data_sum)
        #
        #
        # print(df_sum*(coverage_rate/100))

        return match_data_sum > df_sum * (coverage_rate / 100)

        # data_list  = count_list.dropna()

        # count = df_series.count()
        # coverage_rate = rule.params.coverageRate
        # aggregation = rule.params.aggregation
        # if coverage_rate is not None and aggregation is None:
        #     return count_common_value / count <= coverage_rate / 100
        # elif coverage_rate is None and aggregation is not None:
        #     return

    def get_aggregation_fit_data(aggregation_limit, loop_data):
        match_data_list = []
        for index, count_value in enumerate(loop_data):
            if count_value < aggregation_limit:
                match_data_list.append({"index": index, "count_value": count_value})

            aggregation_limit = aggregation_limit - count_value
            if aggregation_limit < 0:
                break
        return match_data_list

    def factor_common_value_over_coverage(df: DataFrame, topic, rule: MonitorRule):
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
                factor_rule_result.result = common_value_over_coverage(value, rule, factor)
                factor_rule_result_list.append(factor_rule_result)

            execute_result.factorResult = factor_rule_result_list
            return execute_result

    return factor_common_value_over_coverage
