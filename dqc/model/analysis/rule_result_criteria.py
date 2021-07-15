from pydantic.main import BaseModel


class MonitorRuleLogCriteria(BaseModel):
    startDate: str = None
    endDate: str = None
    ruleCode: str = None
    topicId: str = None
    factorId: str = None


class MonitorRuleLogRequest(BaseModel):
    criteria: MonitorRuleLogCriteria = None
