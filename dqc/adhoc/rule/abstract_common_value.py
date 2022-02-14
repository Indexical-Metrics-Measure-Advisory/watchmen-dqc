import logging

from dqc.adhoc.rule.abstract_rule import AbstractRule
import datetime
from abc import abstractmethod, ABC
from typing import Dict, Optional

from dqc.common.utils.data_utils import build_collection_name
from model.model.topic.topic import Topic
from model.model.topic.factor import Factor

from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.presto.presto_client import get_connection

log = logging.getLogger("app." + __name__)


class AbstractCommonValue(AbstractRule, ABC):

    def __init__(self, schema: str, topic: Topic, rule: MonitorRule, factor: Factor,
                 process_date: Optional[datetime.datetime] = None):
        super().__init__(schema, topic, rule, factor, process_date)

    @abstractmethod
    def execute(self) -> bool:
        return True

    def get_value_mode_sql(self) -> str:
        sql = "SELECT {field}, count(*) AS count " \
              "FROM {schema}.{table} " \
              "WHERE update_time_ between timestamp '{from_date}' and timestamp '{to_date}' " \
              "GROUP BY {field} " \
              "ORDER BY {field} DESC " \
              "LIMIT 1".format(
                    field=self.factor.name,
                    schema=self.schema,
                    table=build_collection_name(self.topic.name),
                    from_date=self.from_date.format('YYYY-MM-DD HH:mm:ss'),
                    to_date=self.to_date.format('YYYY-MM-DD HH:mm:ss')
                )
        return sql

    def get_mode_value(self) -> Dict:
        sql = self.get_value_mode_sql()
        log.info(sql)
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        result = {}
        if row:
            for index, value in enumerate(row):
                if index == 0:
                    result["mode_value"] = value
                elif index == 1:
                    result["count"] = value
        return result

    def get_count_value(self):
        sql = self.get_count_sql()
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
