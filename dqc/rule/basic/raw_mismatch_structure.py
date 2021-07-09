import logging

from pandas import DataFrame

from dqc.common.constants import TOPIC_RULE
from dqc.rule.utils.topic_utils import table_not_exist, data_is_empty, init_topic_rule_result

log = logging.getLogger("app." + __name__)


def init():
    def raw_mismatch_structure(df: DataFrame, topic, rule=None):
        if table_not_exist(df) or data_is_empty(df):
            return TOPIC_RULE, None
        else:
            topic_rule_result = init_topic_rule_result(rule, topic)
            factor_list = topic["factors"]
            factor_contains_result = []
            missing_factors = []
            for factor in factor_list:
                factor_name = factor["name"].lower()
                if factor_name not in df.columns:
                    missing_factors.append(factor_name)
                factor_contains_result.append(factor_name in df.columns)
            topic_rule_result.result = False in factor_contains_result
            topic_rule_result.params = {"missing_factors": missing_factors}
            return TOPIC_RULE, topic_rule_result


    return raw_mismatch_structure
