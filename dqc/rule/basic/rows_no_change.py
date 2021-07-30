from pandas import DataFrame

from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.rule.utils.date_utils import get_date_range, get_date_range_with_end_date
from dqc.rule.utils.topic_utils import data_is_empty, table_not_exist, init_topic_rule_result
from dqc.service.query.index import query_topic_data_count_by_datetime


def init():
    def rows_no_change(df: DataFrame, topic, rule: MonitorRule):

        if table_not_exist(df) or data_is_empty(df):
            return None
        else:
            execute_result = init_topic_rule_result(rule, topic)
            statistical_interval = rule.params.statisticalInterval
            # TODO date range
            start_date, end_date = get_date_range(statistical_interval)
            prior_start_date, prior_end_date = get_date_range_with_end_date(statistical_interval, start_date)
            coverage_rate = rule.params.coverageRate
            if coverage_rate is None:
                raise ValueError("coverage rate is None")
            current_count = len(df.index)
            prior_count = query_topic_data_count_by_datetime(topic, prior_start_date, prior_end_date)

            if current_count <= prior_count * (coverage_rate / 100):
                execute_result.topicResult.result = True
            else:
                execute_result.topicResult.result = False

            return execute_result

    return rows_no_change
