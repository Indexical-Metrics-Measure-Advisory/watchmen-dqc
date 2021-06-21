import logging

from pandas import DataFrame

from dqc.common.constants import TOPIC_RULE
from dqc.model.analysis.topic_rule_result import TopicRuleResult
from dqc.rule.utils.topic_utils import table_not_exist, data_is_empty, init_topic_rule_result

log = logging.getLogger("app." + __name__)


def init():

    def raw_match_structure(df: DataFrame, topic, rule=None):
        if table_not_exist(df) or data_is_empty(df):
            return TOPIC_RULE, None
        else:
            topic_rule_result = init_topic_rule_result(rule, topic)
            factor_list = topic["factors"]
            factor_contains_result = []
            for factor in factor_list:
                factor_name = factor["name"].lower()
                factor_contains_result.append(factor_name in df.columns)

            topic_rule_result.result = not False in factor_contains_result

            ## check topic
            return TOPIC_RULE, topic_rule_result

    return raw_match_structure
