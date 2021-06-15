from typing import List

from storage.common.mongo_model import MongoModel

from dqc.model.analysis.expect_rule import ExpectRuleTopic


class TopicRule(MongoModel):
    topicRuleId: str = None
    topicId: str = None
    topicName: str = None
    ruleList: List[ExpectRuleTopic] = []
