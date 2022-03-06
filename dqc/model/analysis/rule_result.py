from typing import List

from model.model.common.watchmen_model import WatchmenModel
from pydantic import BaseModel


class RuleResult(WatchmenModel):
    topicId: str = None
    topicName: str = None
    factorId: str = None
    factorName: str = None
    ruleCode: str = None
    result: bool = None
    severity: str = None
    tenant_id_: str = None
    params: dict = {}


class RuleExecuteResult(BaseModel):
    ruleType: str = None
    topicResult: RuleResult = None
    factorResult: List[RuleResult] = []
    tenantId: str = None
