import datetime
from typing import Optional

from model.model.topic.factor import Factor
from model.model.topic.topic import Topic

from dqc.adhoc.rule.factor_mismatch_type import FactorMismatchType
from dqc.model.analysis.monitor_rule import MonitorRule


class FactorMismatchDateType(FactorMismatchType):

    def __init__(self, schema: str, topic: Topic, rule: MonitorRule, factor: Optional[Factor] = None,
                 process_date: Optional[datetime.datetime] = None):
        super().__init__(schema, topic, rule, factor, process_date)

    def execute(self) -> bool:
        return super().execute()
