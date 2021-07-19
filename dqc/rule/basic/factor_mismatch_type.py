import logging
from typing import List

from pandas import DataFrame

from dqc.common.constants import FACTOR_RULE
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.model.analysis.rule_result import RuleExecuteResult
from dqc.rule.utils.factor_utils import check_value_match_type, find_factor
from dqc.rule.utils.topic_utils import data_is_empty, table_not_exist, init_factor_rule_result

log = logging.getLogger("app." + __name__)


def init():
    def factor_mismatch_type(df: DataFrame, topic: dict, rule: MonitorRule):

        if table_not_exist(df) or data_is_empty(df):
            return None
        else:

            factor_rule_result_list: List = []
            # columns = df.columns
            factor_list = get_execute_factor_list(rule, topic)
            execute_result = RuleExecuteResult()
            execute_result.ruleType = FACTOR_RULE
            for factor in factor_list:
                factor_rule_result = init_factor_rule_result(rule, topic, factor)
                factor_type = factor["type"]
                factor_name = factor["name"].lower()
                if factor_name in df.columns:
                    value = df[factor["name"].lower()]
                    factor_rule_result.result = not check_value_match_type(value, factor_type)
                    factor_rule_result.params[factor["name"]] = "mismatch type"
                    factor_rule_result_list.append(factor_rule_result)
            execute_result.factorResult = factor_rule_result_list
            return execute_result

    def get_execute_factor_list(rule, topic):
        factor_list = topic["factors"]
        if rule.factorId is not None:
            return find_factor(factor_list, rule.factorId)
        elif rule.grade == "global" or rule.grade=="topic":
            return factor_list

    return factor_mismatch_type
