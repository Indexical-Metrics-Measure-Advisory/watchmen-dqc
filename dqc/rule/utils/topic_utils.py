from pandas import DataFrame

from dqc.model.analysis.rule_result import RuleResult


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


def init_factor_rule_result(rule, topic,factor):
    factor_rule_result = RuleResult()
    factor_rule_result.topicId = topic["topicId"]
    factor_rule_result.topicName = topic["name"]
    factor_rule_result.factorId = factor["factorId"]
    factor_rule_result.factorName = factor["name"]
    if rule:
        factor_rule_result.ruleCode = rule.code or ""
        factor_rule_result.severity = rule.severity or ""

    return factor_rule_result

