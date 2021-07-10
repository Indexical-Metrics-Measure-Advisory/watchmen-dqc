import logging

from pandas import DataFrame

from dqc.common.constants import FACTOR_RULE
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.rule.utils.factor_utils import check_value_match_type, find_factor
from dqc.rule.utils.topic_utils import data_is_empty, table_not_exist, init_factor_rule_result

log = logging.getLogger("app." + __name__)





def init():

    def factor_mismatch_type(df: DataFrame, topic: dict, rule: MonitorRule):

        if table_not_exist(df) or data_is_empty(df):
            return FACTOR_RULE, None
        else:
            factor_list = topic["factors"]
            factor_rule_result_list = []

            if rule.factorId is not None:
                factor_list = find_factor(factor_list,rule.factorId)

            for factor in factor_list:
                factor_rule_result = init_factor_rule_result(rule, topic,factor)
                factor_type = factor["type"]
                value = df[factor["name"].lower()]
                factor_rule_result.result = not check_value_match_type(value,factor_type)
                factor_rule_result.params[factor["name"]] = "mismatch type"
                factor_rule_result_list.append(factor_rule_result)

            return FACTOR_RULE,factor_rule_result_list

    return factor_mismatch_type
