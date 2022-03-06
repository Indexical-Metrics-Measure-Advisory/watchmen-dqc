import datetime
import logging
from typing import Optional

from model.model.topic.factor import Factor
from model.model.topic.topic import Topic

from dqc.adhoc.rule.abstract_rule import AbstractRule
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.presto.presto_client import get_connection

log = logging.getLogger("app." + __name__)


class FactorEmptyOverCoverage(AbstractRule):

    def __init__(self, schema: str, topic: Topic, rule: MonitorRule, factor: Optional[Factor] = None,
                 process_date: Optional[datetime.datetime] = None):
        super().__init__(schema, topic, rule, factor, process_date)

    def execute(self) -> bool:
        super().execute()
        if self.rule.params.coverageRate:
            return self.match_empty_over_coverage() < self.rule.params.coverageRate
        else:
            return False

    def get_empty_sql(self) -> str:
        criterion_sql = "{field} IS NULL".format(
            field=self.factor.name
        )
        query_sql = self.get_count_with_criterion_sql(criterion_sql)
        return query_sql

    def match_empty_over_coverage(self):
        change_sql = self.get_empty_sql()
        log.info("empty sql: {sql}".format(sql=change_sql))
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(change_sql)
        empty_count_row = cur.fetchone()
        empty_count_result = self.get_count_result(empty_count_row)

        total_sql = self.get_count_sql()
        log.info("total sql: {sql}".format(sql=total_sql))
        cur.execute(total_sql)
        total_count_row = cur.fetchone()
        total_count_result = self.get_count_result(total_count_row)

        return (empty_count_result["count"] / total_count_result["count"]) * 100
