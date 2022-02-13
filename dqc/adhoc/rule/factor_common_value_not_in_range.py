import datetime
from typing import Optional

from dqc.adhoc.rule.abstract_common_value import AbstractCommonValue
from dqc.adhoc.utils import get_from_to_date

from model.model.topic.topic import Topic
from model.model.topic.factor import Factor

from dqc.model.analysis.monitor_rule import MonitorRule


class FactorCommonValueNotInRange(AbstractCommonValue):

    def __init__(self, schema: str, topic: Topic, rule: MonitorRule, factor: Factor,
                 process_date: Optional[datetime.datetime] = None):
        super().__init__(schema, topic, rule, factor, process_date)

    def execute(self) -> bool:
        aggregation = self.rule.params.aggregation
        if aggregation is None:
            raise ValueError("aggregation is null")
        if self.rule.params.min is None or self.rule.params.max is None:
            raise ValueError("min {0} or max {1} is None".format(self.rule.params.min, self.rule.params.max))

        range_min = int(self.rule.params.min)
        range_max = int(self.rule.params.max)
        self.from_date, self.to_date = get_from_to_date(self.rule.params.statisticalInterval, self.process_date)

        mode_value_result = self.get_mode_value()
        if mode_value_result:
            common_value_count = mode_value_result["count"]
            result = range_min < common_value_count < range_max
            return result

