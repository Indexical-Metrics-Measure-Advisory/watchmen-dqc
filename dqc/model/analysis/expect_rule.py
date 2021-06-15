from pydantic import BaseModel


class ExpectRuleFactor(BaseModel):
    # columnId: str = None
    # factorId: str = None
    expectColumnRule: str = None
    ruleCondition: list = []


class ExpectRuleTopic(BaseModel):
    # topicId: str = None
    expectColumnRule: str = None
    ruleCondition: list = []
