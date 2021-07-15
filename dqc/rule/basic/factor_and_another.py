from pandas import DataFrame

from dqc.model.analysis.monitor_rule import MonitorRule


def init():
    def factor_and_another(df: DataFrame, topic, rule: MonitorRule):
        raise NotImplementedError("factor_and_another not implemented yet")

    return factor_and_another
