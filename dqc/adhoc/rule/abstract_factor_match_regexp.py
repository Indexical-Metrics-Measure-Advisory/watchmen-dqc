import logging
from abc import abstractmethod

from dqc.adhoc.rule.abstract_rule import AbstractRule
import datetime
from typing import Optional, Tuple

from model.model.topic.topic import Topic
from model.model.topic.factor import Factor

from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.presto.presto_client import get_connection

log = logging.getLogger("app." + __name__)


class AbstractFactorMatchRegexp(AbstractRule):

    def __init__(self, schema: str, topic: Topic, rule: MonitorRule, factor: Optional[Factor] = None,
                 process_date: Optional[datetime.datetime] = None):
        super().__init__(schema, topic, rule, factor, process_date)

    @abstractmethod
    def execute(self) -> bool:
        super().execute()
        pass

    def get_factor_match_regexp_sql(self, pattern) -> str:
        criterion_sql = "regexp_like({field}, '{pattern}')".format(
            field=self.factor.name,
            pattern=pattern
        )
        query_sql = self.get_count_with_criterion_sql(criterion_sql)
        return query_sql

    def get_match_count_and_total_count(self, pattern) -> Tuple:
        match_regexp_sql = self.get_factor_match_regexp_sql(pattern)
        log.info("factor match regexp: {sql}".format(sql=match_regexp_sql))
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(match_regexp_sql)
        match_count_row = cursor.fetchone()
        match_count_result = self.get_count_result(match_count_row)

        total_sql = self.get_count_sql()
        log.info("total sql: {sql}".format(sql=total_sql))
        cursor.execute(total_sql)
        total_count_row = cursor.fetchone()
        total_count_result = self.get_count_result(total_count_row)
        return match_count_result["count"], total_count_result["count"]
