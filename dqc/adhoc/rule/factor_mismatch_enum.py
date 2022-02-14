import logging

from dqc.adhoc.rule.abstract_rule import AbstractRule
import datetime
from typing import Optional

from model.model.topic.topic import Topic
from model.model.topic.factor import Factor

from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.presto.presto_client import get_connection

log = logging.getLogger("app." + __name__)

class FactorMismatchEnum(AbstractRule):

    def __init__(self, schema: str, topic: Topic, rule: MonitorRule, factor: Optional[Factor] = None,
                 process_date: Optional[datetime.datetime] = None):
        super().__init__(schema, topic, rule, factor, process_date)

    def execute(self) -> bool:
        super().execute()
        match_result = self.get_mismatch_enum()
        result = match_result["count"] == 0
        return result

    def get_mismatch_enum_sql(self):
        # Todo Just Sample
        criterion_sql = "'{field}' <> ANY (VALUES '{value1}', '{value2}')".format(
            # field=self.factor.name,
            field="value1",
            value1="value1",
            value2="value2"
        )
        query_sql = self.get_count_with_criterion_sql(criterion_sql)
        return query_sql

    def get_mismatch_enum(self):
        sql = self.get_mismatch_enum_sql()
        log.info(sql)
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        result = self.get_count_result(row)
        return result
