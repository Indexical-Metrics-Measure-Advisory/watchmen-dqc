import logging

from dqc.adhoc.rule.abstract_rule import AbstractRule
import datetime
from typing import Optional

from model.model.topic.topic import Topic
from model.model.topic.factor import Factor

from dqc.common.utils.data_utils import build_collection_name
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.presto.presto_client import get_connection

log = logging.getLogger("app." + __name__)


class RowsCountMismatchAndAnother(AbstractRule):

    def __init__(self, schema: str, topic: Topic, rule: MonitorRule, another_topic: Topic,
                 factor: Optional[Factor] = None,
                 process_date: Optional[datetime.datetime] = None):
        super().__init__(schema, topic, rule, factor, process_date)
        self.another_topic = another_topic

    def execute(self) -> bool:
        super().execute()
        return self.match_rows_count_mismatch_and_another()

    def get_topic_count_sql(self) -> str:
        return self.get_count_sql()

    def get_another_topic_count_sql(self) -> str:
        sql = "SELECT count(*) as count " \
              "FROM {schema}.{table} " \
              "WHERE update_time_ between timestamp '{from_date}' and timestamp '{to_date}'".format(
            schema=self.schema,
            table=build_collection_name(self.another_topic.name),
            from_date=self.from_date.format('YYYY-MM-DD HH:mm:ss'),
            to_date=self.to_date.format('YYYY-MM-DD HH:mm:ss')
        )
        return sql

    def match_rows_count_mismatch_and_another(self) -> bool:
        topic_count_sql = self.get_topic_count_sql()
        log.info("topic count sql: {sql}".format(sql=topic_count_sql))
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(topic_count_sql)
        topic_count_row = cur.fetchone()
        topic_count_result = self.get_count_result(topic_count_row)

        another_topic_count_sql = self.get_another_topic_count_sql()
        log.info("another topic count sql: {sql}".format(sql=another_topic_count_sql))
        cur.execute(another_topic_count_sql)
        another_topic_count_row = cur.fetchone()
        another_topic_count_result = self.get_count_result(another_topic_count_row)

        return topic_count_result == another_topic_count_result
