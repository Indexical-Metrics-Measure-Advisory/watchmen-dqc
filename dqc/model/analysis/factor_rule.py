from typing import List

from storage.common.mongo_model import MongoModel

from dqc.model.analysis.expect_rule import ExpectRuleFactor


class FactorRule(MongoModel):
    factorRuleId: str = None
    topicId: str = None
    factorName: str = None
    factorId: str = None
    ruleList: List[ExpectRuleFactor] = []
