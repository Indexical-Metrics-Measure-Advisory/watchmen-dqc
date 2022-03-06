import datetime
import logging
from typing import Optional, Dict

from model.model.topic.factor import Factor
from model.model.topic.topic import Topic

from dqc.adhoc.rule.abstract_rule import AbstractRule
from dqc.common.utils.data_utils import build_collection_name
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.presto.presto_client import get_connection

log = logging.getLogger("app." + __name__)


class RowsNotExists(AbstractRule):

    def __init__(self,
                 schema: str,
                 topic: Topic,
                 rule: MonitorRule,
                 factor: Optional[Factor] = None,
                 process_date: Optional[datetime.datetime] = None):
        super().__init__(schema, topic, rule, factor, process_date)

    def execute(self) -> bool:
        match_result = self.match_rows_not_exists()
        if match_result["count"] == 0:
            return False
        else:
            return True

    def get_rows_not_exists_sql(self) -> str:
        sql = "SELECT count(*) as count " \
              "FROM {schema}.{table} ".format(
            schema=self.schema,
            table=build_collection_name(self.topic.name)
        )
        return sql

    def match_rows_not_exists(self) -> Dict:
        sql = self.get_rows_not_exists_sql()
        log.info(sql)
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        result = {}
        if row:
            for index, value in enumerate(row):
                if index == 0:
                    result["count"] = value
        return result
