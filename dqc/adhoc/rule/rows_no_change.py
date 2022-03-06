import datetime
import logging
from typing import Optional

from model.model.topic.factor import Factor
from model.model.topic.topic import Topic

from dqc.adhoc.rule.abstract_rule import AbstractRule
from dqc.common.utils.data_utils import build_collection_name
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.presto.presto_client import get_connection

log = logging.getLogger("app." + __name__)


class RowsNoChange(AbstractRule):

    def __init__(self, schema: str, topic: Topic, rule: MonitorRule, factor: Optional[Factor] = None,
                 process_date: Optional[datetime.datetime] = None):
        super().__init__(schema, topic, rule, factor, process_date)

    def execute(self) -> bool:
        super().execute()
        if self.rule.params.coverageRate:
            return self.match_rows_no_change() < self.rule.params.coverageRate
        else:
            return False

    def get_rows_change_sql(self) -> str:
        return self.get_count_sql()

    def get_total_count_sql(self) -> str:
        sql = "SELECT count(*) as count " \
              "FROM {schema}.{table}".format(
            schema=self.schema,
            table=build_collection_name(self.topic.name)
        )
        return sql

    def match_rows_no_change(self):
        change_sql = self.get_rows_change_sql()
        log.info("rows change sql: {sql}".format(sql=change_sql))
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(change_sql)
        change_count_row = cur.fetchone()
        change_count_result = self.get_count_result(change_count_row)

        total_sql = self.get_total_count_sql()
        log.info("total sql: {sql}".format(sql=total_sql))
        cur.execute(total_sql)
        total_count_row = cur.fetchone()
        total_count_result = self.get_count_result(total_count_row)

        return (change_count_result["count"] / total_count_result["count"]) * 100
