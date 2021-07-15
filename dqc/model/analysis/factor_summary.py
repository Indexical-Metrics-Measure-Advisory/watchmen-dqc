from datetime import datetime

from dqc.model.summary_model import SummaryModel


class FactorSummary(SummaryModel):
    factorId: str = None
    factorName: str = None
    distinctValueNumber: int = None
    valueMinimum: int = None
    valueMaximum: int = None
    valueTypeMissing: bool = False
    nullPercent: float = None
    numberOfNull: int = None
    isUnique: bool = None
    insertDateTime: datetime = None
