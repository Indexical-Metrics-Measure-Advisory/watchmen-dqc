from pandas import DataFrame

from dqc.model.analysis.monitor_rule import MonitorRule


def init():
    def factor_breaks_monotone_decreasing(df: DataFrame, topic, rule: MonitorRule):
        raise NotImplementedError("factor_breaks_monotone_decreasing not implemented yet")

    return factor_breaks_monotone_decreasing
