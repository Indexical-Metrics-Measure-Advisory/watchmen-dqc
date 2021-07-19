

from pandas import DataFrame

from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.rule.utils.factor_utils import check_match_regex
from dqc.rule.utils.topic_utils import check_factor_value


def init():
    def factor_match_regexp(df: DataFrame, topic, rule: MonitorRule):
        return check_factor_value(df, topic, rule, check_match_regex)

    return factor_match_regexp
