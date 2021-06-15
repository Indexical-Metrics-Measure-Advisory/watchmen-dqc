from pydantic import BaseModel

from dqc.model.analysis.factor_summary import FactorSummary


class Factor(BaseModel):
    factorId: str = None
    topicId: str = None
    name: str = None
    description: str = None
    factorType: str = None
    factorSummary: FactorSummary = None
