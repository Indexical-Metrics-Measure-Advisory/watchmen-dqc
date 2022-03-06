import datetime
import logging
from typing import Optional

from model.model.topic.factor import Factor
from model.model.topic.topic import Topic

from dqc.adhoc.rule.abstract_rule import AbstractRule
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.presto.presto_client import get_connection

log = logging.getLogger("app." + __name__)


class FactorAndAnother(AbstractRule):

    def __init__(self, schema: str, topic: Topic, rule: MonitorRule,
                 factor: Optional[Factor] = None,
                 process_date: Optional[datetime.datetime] = None):
        super().__init__(schema, topic, rule, factor, process_date)

    def execute(self) -> bool:
        super().execute()
        if self.rule.params.factorId:
            another_factor = self.get_factor(self.topic, self.rule.params.factorId)
            if self.factor.type == another_factor.type:
                return self.match_factor_match_another(self.factor.name, another_factor.name)
        else:
            return False

    def get_factor_and_another_sql(self, field1, field2) -> str:
        criterion_sql = "{field1} = {field2} ".format(
            field1=field1,
            field2=field2
        )
        query_sql = self.get_count_with_criterion_sql(criterion_sql)
        return query_sql

    def match_factor_match_another(self, filed1: str, field2: str) -> bool:
        match_another_sql = self.get_factor_and_another_sql(filed1, field2)
        log.info("factor and another: {sql}".format(sql=match_another_sql))
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(match_another_sql)
        match_count_row = cursor.fetchone()
        match_count_result = self.get_count_result(match_count_row)

        total_sql = self.get_count_sql()
        log.info("total sql: {sql}".format(sql=total_sql))
        cursor.execute(total_sql)
        total_count_row = cursor.fetchone()
        total_count_result = self.get_count_result(total_count_row)
        return match_count_result["count"] == total_count_result["count"]
