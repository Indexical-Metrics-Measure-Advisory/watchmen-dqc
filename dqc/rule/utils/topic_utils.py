from pandas import DataFrame

from dqc.model.analysis.topic_rule_result import TopicRuleResult


def table_not_exist(df:DataFrame):
    return df  is None


def data_is_empty(df:DataFrame):
    return df.empty


def init_topic_rule_result(rule, topic):
    topic_rule_result = TopicRuleResult()
    topic_rule_result.topicId = topic["topicId"]
    topic_rule_result.topicName = topic["name"]
    if rule:
        topic_rule_result.ruleCode = rule.code or ""
        topic_rule_result.severity = rule.severity or ""

    return topic_rule_result