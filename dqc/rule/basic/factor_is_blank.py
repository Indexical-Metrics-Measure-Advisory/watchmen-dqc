from pandas import DataFrame

from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.rule.utils.factor_utils import check_is_empty
from dqc.rule.utils.topic_utils import check_factor_value


def init():
    ## TODO check is blank
    def factor_is_empty(df: DataFrame, topic, rule: MonitorRule):
        return check_factor_value(df, topic, rule, check_is_empty)

    return factor_is_empty
