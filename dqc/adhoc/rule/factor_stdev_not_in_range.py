import datetime
import logging
from typing import Optional

from model.model.topic.factor import Factor
from model.model.topic.topic import Topic

from dqc.adhoc.rule.abstract_factor_use_pandas import AbstractFactorUsePandas
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.presto.presto_client import get_connection

log = logging.getLogger("app." + __name__)


class FactorStdevNotInRange(AbstractFactorUsePandas):

    def __init__(self, schema: str, topic: Topic, rule: MonitorRule, factor: Optional[Factor] = None,
                 process_date: Optional[datetime.datetime] = None):
        super().__init__(schema, topic, rule, factor, process_date)

    def execute(self) -> bool:
        super().execute()
        return self.match_stdev_not_in_range()

    def match_stdev_not_in_range(self) -> bool:
        query_sql = self.get_factor_data_sql()
        log.info("factor stdev not in range: {sql}".format(sql=query_sql))
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query_sql)
        rows = cursor.fetchall()
        data_frame = self.get_data_frame(cursor, rows)
        std = data_frame[self.factor.name.lower()].std()
        return self.check_value_not_in_range(std)
