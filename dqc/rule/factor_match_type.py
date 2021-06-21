import logging

from pandas import DataFrame

from dqc.common.constants import FACTOR_RULE
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.rule.utils.factor_utils import check_value_match_type
from dqc.rule.utils.topic_utils import data_is_empty, table_not_exist

log = logging.getLogger("app." + __name__)


def init():
    def factor_match_type(df: DataFrame, topic: dict, rule: MonitorRule):

        if table_not_exist(df) or data_is_empty(df):
            return FACTOR_RULE, None
        else:
            factor_list = topic["factors"]
            for factor in factor_list:
                print(factor)
                factor_type = factor["type"]
                value = df[factor["name"]]
                # print(type(value))
                print("check rule",check_value_match_type(value,factor_type))

    return factor_match_type
