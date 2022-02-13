import logging

from dqc.adhoc.rule.abstract_rule import AbstractRule
import datetime
from typing import Optional, Dict

from model.model.topic.topic import Topic
from model.model.topic.factor import Factor

from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.presto.presto_client import get_connection

log = logging.getLogger("app." + __name__)


class FactorStringLengthMismatch(AbstractRule):

    def __init__(self, schema: str, topic: Topic, rule: MonitorRule, factor: Optional[Factor] = None,
                 process_date: Optional[datetime.datetime] = None):
        super().__init__(schema, topic, rule, factor, process_date)

    def execute(self) -> bool:
        super().execute()
        if self.rule.params:
            match_result = self.match_string_length_mismatch(self.rule.params.length)
            return match_result["count"] != 0
        else:
            return True

    def get_string_length_mismatch_sql(self, length) -> str:
        criterion_sql = "length({field}) != {length} ".format(
            field=self.factor.name,
            length=length
        )
        query_sql = self.get_count_with_criterion_sql(criterion_sql)
        return query_sql

    def match_string_length_mismatch(self, length) -> Dict:
        sql = self.get_string_length_mismatch_sql(length)
        log.info("string length mismatch: {sql}".format(sql=sql))
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        result = self.get_count_result(row)
        return result
