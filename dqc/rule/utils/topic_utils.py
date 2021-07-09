from pandas import DataFrame

from dqc.common.constants import FACTOR_RULE
from dqc.model.analysis.rule_result import RuleResult
from dqc.rule.utils.factor_utils import find_factor


def table_not_exist(df: DataFrame):
    return df is None


def data_is_empty(df: DataFrame):
    return df.empty


def init_topic_rule_result(rule, topic):
    topic_rule_result = RuleResult()
    topic_rule_result.topicId = topic["topicId"]
    topic_rule_result.topicName = topic["name"]
    if rule:
        topic_rule_result.ruleCode = rule.code or ""
        topic_rule_result.severity = rule.severity or ""

    return topic_rule_result


def init_factor_rule_result(rule, topic, factor):
    factor_rule_result = RuleResult()
    factor_rule_result.topicId = topic["topicId"]
    factor_rule_result.topicName = topic["name"]
    factor_rule_result.factorId = factor["factorId"]
    factor_rule_result.factorName = factor["name"]
    if rule:
        factor_rule_result.ruleCode = rule.code or ""
        factor_rule_result.severity = rule.severity or ""

    return factor_rule_result


def check_factor_value(df, topic, rule, check_function):
    if table_not_exist(df) or data_is_empty(df):
        return FACTOR_RULE, None
    else:
        factor_rule_result_list = []
        factor_filtered = get_execute_factor_list(rule, topic)
        for factor in factor_filtered:
            factor_rule_result = init_factor_rule_result(rule, topic, factor)
            value = df[factor["name"].lower()]
            factor_rule_result.result = not check_function(value,rule,factor)
            factor_rule_result_list.append(factor_rule_result)
        return FACTOR_RULE, factor_rule_result_list


def get_execute_factor_list(rule, topic):
    factor_list = topic["factors"]
    if rule.factorId is not None:
        return find_factor(factor_list, rule.factorId)
    else:
        return factor_list
