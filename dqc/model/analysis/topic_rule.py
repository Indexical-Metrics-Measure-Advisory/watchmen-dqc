from typing import List

from dqc.model.analysis.expect_rule import ExpectRuleTopic
from storage.common.mongo_model import MongoModel


class TopicRule(MongoModel):
    topicRuleId: str = None
    topicId: str = None
    topicName: str = None
    ruleList: List[ExpectRuleTopic] = []
