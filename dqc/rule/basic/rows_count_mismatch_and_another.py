from pandas import DataFrame

from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.rule.utils.date_utils import get_date_range
from dqc.rule.utils.topic_utils import data_is_empty, table_not_exist, init_topic_rule_result
from dqc.sdk.admin.admin_sdk import get_topic_by_id
from dqc.service.query.index import query_topic_data_count_by_datetime


def init():
    def rows_count_mismatch_and_another(df: DataFrame, topic, rule: MonitorRule):
        execute_result = init_topic_rule_result(rule, topic)
        if table_not_exist(df) or data_is_empty(df):
            return None

        start_date, end_date = get_date_range(rule.params.statisticalInterval)
        topic_id = rule.params.topicId
        another_topic = get_topic_by_id(topic_id)
        current_count = len(df.index)
        prior_count = query_topic_data_count_by_datetime(another_topic, start_date, end_date)
        if current_count != prior_count:
            execute_result.topicResult.result = True
        else:
            execute_result.topicResult.result = False

        return execute_result

    return rows_count_mismatch_and_another
