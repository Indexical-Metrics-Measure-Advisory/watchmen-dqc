import datetime
import logging
from typing import Optional, Dict

from model.model.topic.factor import Factor
from model.model.topic.topic import Topic

from dqc.adhoc.rule.abstract_rule import AbstractRule
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.presto.presto_client import get_connection

log = logging.getLogger("app." + __name__)


class FactorNotInRange(AbstractRule):

    def __init__(self, schema: str, topic: Topic, rule: MonitorRule, factor: Optional[Factor] = None,
                 process_date: Optional[datetime.datetime] = None):
        super().__init__(schema, topic, rule, factor, process_date)

    def execute(self) -> bool:
        super().execute()
        if self.rule.params:
            if self.rule.params.min and self.rule.params.max:
                result = self.match_factor_not_in_range(self.rule.params.min,
                                                        self.rule.params.max)
                return result["count"] != 0
            else:
                return True
        else:
            return True

    def get_factor_not_in_range_sql(self, min_, max_) -> str:
        criterion_sql = "{field} not between {min} and {max}".format(
            field=self.factor.name,
            min=min_,
            max=max_
        )
        sql = self.get_count_with_criterion_sql(criterion_sql)
        return sql

    def match_factor_not_in_range(self, min_, max_) -> Dict:
        query_sql = self.get_factor_not_in_range_sql(min_, max_)
        log.info("factor not in range: {sql}".format(sql=query_sql))
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query_sql)
        row = cursor.fetchone()
        result = self.get_count_result(row)
        return result
