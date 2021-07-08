from pydantic import BaseModel


class RuleResult(BaseModel):
    topicId: str = None
    topicName: str = None
    factorId :str = None
    factorName :str = None
    ruleCode: str = None
    result: bool = None
    severity:str= None
    params:dict = {}
