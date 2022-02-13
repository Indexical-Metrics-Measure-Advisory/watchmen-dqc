import logging

from dqc.adhoc.rule.abstract_factor_match_regexp import AbstractFactorMatchRegexp
import datetime
from typing import Optional

from model.model.topic.topic import Topic
from model.model.topic.factor import Factor

from dqc.model.analysis.monitor_rule import MonitorRule


log = logging.getLogger("app." + __name__)


class FactorMatchRegexp(AbstractFactorMatchRegexp):

    def __init__(self, schema: str, topic: Topic, rule: MonitorRule, factor: Optional[Factor] = None,
                 process_date: Optional[datetime.datetime] = None):
        super().__init__(schema, topic, rule, factor, process_date)

    def execute(self) -> bool:
        super().execute()
        if self.rule.params:
            match_count_result, total_count_result = self.get_match_count_and_total_count(self.rule.params.regexp)
            return match_count_result == total_count_result
        else:
            return False

