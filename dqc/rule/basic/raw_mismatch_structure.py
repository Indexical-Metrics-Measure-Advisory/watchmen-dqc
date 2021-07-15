import logging

from pandas import DataFrame

from dqc.common.constants import TOPIC_RULE
from dqc.rule.utils.topic_utils import table_not_exist, data_is_empty, init_topic_rule_result

log = logging.getLogger("app." + __name__)
##TODO

def init():
    def raw_mismatch_structure(df: DataFrame, topic, rule=None):
        raise NotImplementedError("raw_mismatch_structure error")


    return raw_mismatch_structure
