from model.model.common.watchmen_model import WatchmenModel
from pydantic import BaseModel


class MonitorRuleParameters(BaseModel):
    statisticalInterval: str = None
    coverageRate: int = None
    aggregation: int = None
    quantile: int = None
    length: int = None
    max: int = None
    min: int = None
    regexp: str = None
    compareOperator: str = None
    topicId: str = None
    factorId: str = None


class MonitorRule(WatchmenModel):
    ruleId: str = None
    code: str = None
    grade: str = None
    severity: str = None
    enabled: bool = False
    topicId: str = None
    factorId: str = None
    tenantId: str = None
    params: MonitorRuleParameters = None
