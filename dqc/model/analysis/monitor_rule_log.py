from datetime import datetime

from pydantic.main import BaseModel


class MonitorRuleLog(BaseModel):
    ruleCode: str = None
    topicId: str = None
    factorId: str = None
    count: int = None
    lastOccurredTime: datetime = None
