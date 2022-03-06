import datetime

from model.model.topic.factor import Factor
from model.model.topic.topic import Topic

from dqc.adhoc.rule.abstract_common_value import AbstractCommonValue
from dqc.adhoc.utils import get_from_to_date
from dqc.model.analysis.monitor_rule import MonitorRule


class FactorCommonValueOverCoverage(AbstractCommonValue):

    def __init__(self,
                 schema: str,
                 topic: Topic,
                 rule: MonitorRule,
                 factor: Factor,
                 process_date: datetime.datetime = None):
        super().__init__(schema, topic, rule, factor, process_date)

    def execute(self) -> bool:
        coverage_rate = self.rule.params.coverageRate
        if coverage_rate:
            self.from_date, self.to_date = get_from_to_date(self.rule.params.statisticalInterval, self.process_date)
            mode_value_result = self.get_mode_value()
            if mode_value_result:
                common_value_count = mode_value_result["count"]
                count_value_result = self.get_count_value()
                total_count = count_value_result["count"]
                result = (common_value_count / total_count) * 100 < coverage_rate
                if result:
                    return True
                else:
                    return False
            else:
                return True
        else:
            return False
