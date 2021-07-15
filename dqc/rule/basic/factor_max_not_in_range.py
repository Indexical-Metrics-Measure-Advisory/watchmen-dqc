
from pandas import DataFrame

from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.rule.utils.factor_utils import check_max_in_range

from dqc.rule.utils.topic_utils import check_factor_value


def init():
    def factor_max_not_in_range(df: DataFrame, topic, rule: MonitorRule):
        return not check_factor_value(df, topic, rule, check_max_in_range)

    return factor_max_not_in_range