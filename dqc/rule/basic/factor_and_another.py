
from pandas import DataFrame

from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.rule.utils.factor_utils import check_avg_in_range

from dqc.rule.utils.topic_utils import check_factor_value


def init():
    def factor_and_another(df: DataFrame, topic, rule: MonitorRule):
        raise NotImplementedError("factor_and_another not implemented yet")

    return factor_and_another
