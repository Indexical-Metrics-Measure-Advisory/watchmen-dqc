from pydantic import BaseModel


class TopicRuleResult(BaseModel):
    topicId: str = None
    topicName: str = None
    ruleCode: str = None
    result: bool = None
    severity:str= None
    params:dict = {}
