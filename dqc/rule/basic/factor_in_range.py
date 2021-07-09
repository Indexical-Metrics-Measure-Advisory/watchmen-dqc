
from pandas import DataFrame

from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.rule.utils.factor_utils import check_value_range

from dqc.rule.utils.topic_utils import check_factor_value


def init():
    def factor_in_range(df: DataFrame, topic, rule: MonitorRule):
        return check_factor_value(df, topic, rule, check_value_range)

    return factor_in_range
