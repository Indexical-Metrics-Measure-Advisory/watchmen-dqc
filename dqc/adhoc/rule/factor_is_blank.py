import datetime
import logging
from typing import Optional, Dict

from model.model.topic.factor import Factor
from model.model.topic.topic import Topic

from dqc.adhoc.rule.abstract_rule import AbstractRule
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.presto.presto_client import get_connection

log = logging.getLogger("app." + __name__)


class FactorIsBlank(AbstractRule):

    def __init__(self, schema: str, topic: Topic, rule: MonitorRule, factor: Optional[Factor] = None,
                 process_date: Optional[datetime.datetime] = None):
        super().__init__(schema, topic, rule, factor, process_date)

    def execute(self) -> bool:
        super().execute()
        match_result = self.match_factor_is_blank()
        return match_result["count"] != 0

    def get_factor_is_blank_sql(self) -> str:
        criterion_sql = "{field} = '' or {field} = ' ' ".format(
            field=self.factor.name
        )
        query_sql = self.get_count_with_criterion_sql(criterion_sql)
        return query_sql

    def match_factor_is_blank(self) -> Dict:
        sql = self.get_factor_is_blank_sql()
        log.info("factor is blank: {sql}".format(sql=sql))
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        result = self.get_count_result(row)
        return result
