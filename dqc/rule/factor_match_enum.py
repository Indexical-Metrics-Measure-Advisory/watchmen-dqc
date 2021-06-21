from pandas import DataFrame

from dqc.common.constants import TOPIC_RULE, FACTOR_RULE
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.model.analysis.topic_rule_result import TopicRuleResult




def init():
    def factor_match_enum(df: DataFrame, topic,rule:MonitorRule):





        # topic_rule_result = TopicRuleResult()
        # topic_rule_result.topicId = topic["topicId"]
        # topic_rule_result.topicName = topic["name"]
        # topic_rule_result.ruleCode = rule.code
        # topic_rule_result.severity = rule.severity
        # if df is None or df.empty:
        #     topic_rule_result.result = True
        # else:
        #     topic_rule_result.result = False

        return FACTOR_RULE, {}
    return factor_match_enum
