import datetime
import logging
from typing import Optional, Dict

from model.model.topic.topic import Topic
from model.model.topic.factor import Factor

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

    def get_factor_not_in_range_sql(self, min_, max_) -> str:
        criterion_sql = "{field} not between {min} and {max}".format(
            field=self.factor.name,
            min=min_,
            max=max_
        )
        sql = self.get_count_with_criterion_sql(criterion_sql)
        return sql

    def match_factor_not_range(self) -> Dict:
        query_sql = self.get_factor_not_in_range_sql()
        log.info("factor not in range: {sql}".format(sql=query_sql))
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query_sql)
        row = cursor.fetchone()
        result = self.get_count_result(row)
        return result
