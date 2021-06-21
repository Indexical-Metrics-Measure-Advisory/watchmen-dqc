import logging
from typing import List

import arrow
from storage.storage.storage_template import list_all, insert_one, find_

from dqc.common.constants import TOPIC_RULES, FACTOR_RULES, TOPIC_RULE, FACTOR_RULE
from dqc.ml.knn.index import run_knn
from dqc.model.analysis.factor_rule import FactorRule
from dqc.model.analysis.topic_rule import TopicRule
from dqc.model.analysis.factor_summary import FactorSummary
from dqc.model.analysis.topic_summary import TopicSummary
from dqc.model.catalog.topic_knn_result import TopicKnnResult
from dqc.service.query.index import query_topic_data_by_datetime
from dqc.rule.index import find_rule_func

REPORT_FACTOR_SUMMARY = "report_factor_summary"

REPORT_TOPIC_SUMMARY = "report_topic_summary"

log = logging.getLogger("app." + __name__)


def __load_topic_rules():
    topic_rules = list_all(TopicRule, TOPIC_RULES)
    return topic_rules


def __build_topic_name(topic_name):
    return "topic_" + topic_name


def __run_rule(rule_column, rule_type):
    rule_func = find_rule_func(rule_column.expectColumnRule, rule_type)
    if rule_column.ruleCondition is not None or not rule_column.ruleCondition:
        return rule_func(*rule_column.ruleCondition)
    else:
        return rule_func()


def __load_factor_rules(topic_id) -> List[FactorRule]:
    factor_rules = find_({"topicId": topic_id}, FactorRule, FACTOR_RULES)
    return factor_rules


def __run_kmeans(data_frame):
    result = TopicKnnResult()
    cluster_num = run_knn(data_frame)
    result.clusterNum = cluster_num
    return result


def run_expect_rule_set_for_topic_data():
    topic_rules: List[TopicRule] = __load_topic_rules()
    for topic_rule in topic_rules:
        data_frame = query_topic_data_by_datetime(__build_topic_name(topic_rule.topicName), None, None)
        topic_summary = TopicSummary()
        for rule in topic_rule.ruleList:
            rule.ruleCondition = [data_frame, topic_summary]
            topic_summary = __run_rule(rule, TOPIC_RULE)
            topic_summary.topicId = topic_rule.topicId
            topic_summary.topicKnnResult = __run_kmeans(data_frame)
            topic_summary.summaryGeneratorDate = arrow.now().datetime.replace(tzinfo=None)
            insert_one(topic_summary, TopicSummary, REPORT_TOPIC_SUMMARY)
            factor_rules = __load_factor_rules(topic_rule.topicId)
            for column in data_frame.columns:
                for factor_rule in factor_rules:
                    if factor_rule.factorName.lower() == column:
                        for rule_expression in factor_rule.ruleList:
                            factor_summary = FactorSummary()
                            factor_summary.factorId = factor_rule.factorId
                            factor_summary.factorName=factor_rule.factorName.lower()
                            rule_expression.ruleCondition = [data_frame, factor_summary]
                            factor_summary = __run_rule(rule_expression, FACTOR_RULE)
                            insert_one(factor_summary, FactorSummary, REPORT_FACTOR_SUMMARY)




## read conf data

## read data from prestod

## run rules

## build topic summary

## build factor summary

## send to watchmen doll for dashboard and alarm


# run_expect_rule_set_for_topic_data()