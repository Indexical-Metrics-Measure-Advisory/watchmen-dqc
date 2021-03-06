import datetime
import logging
from decimal import Decimal
from typing import Optional, Dict

from model.model.topic.factor import Factor
from model.model.topic.topic import Topic

from dqc.adhoc.rule.abstract_rule import AbstractRule
from dqc.common.utils.data_utils import build_collection_name
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.presto.presto_client import get_connection

log = logging.getLogger("app." + __name__)


class FactorMaxNotInRange(AbstractRule):

    def __init__(self, schema: str, topic: Topic, rule: MonitorRule, factor: Optional[Factor] = None,
                 process_date: Optional[datetime.datetime] = None):
        super().__init__(schema, topic, rule, factor, process_date)

    def execute(self) -> bool:
        super().execute()
        if self.rule.params:
            if self.rule.params.min and self.rule.params.max:
                result = self.match_factor_max_not_in_range()
                return not (self.rule.params.min <= result["max"] <= self.rule.params.max)
            else:
                return False
        else:
            return False

    def get_factor_max_not_in_range_sql(self) -> str:
        sql = "SELECT max({field}, 1) as max " \
              "FROM {schema}.{table} " \
              "WHERE update_time_ between timestamp '{from_date}' and timestamp '{to_date}'".format(
            field=self.factor.name.lower(),
            schema=self.schema,
            table=build_collection_name(self.topic.name),
            from_date=self.from_date.format('YYYY-MM-DD HH:mm:ss'),
            to_date=self.to_date.format('YYYY-MM-DD HH:mm:ss')
        )
        return sql

    def match_factor_max_not_in_range(self) -> Dict:
        query_sql = self.get_factor_max_not_in_range_sql()
        log.info("factor max not in range: {sql}".format(sql=query_sql))
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query_sql)
        row = cursor.fetchone()
        result = self.get_max_result(row)
        return result

    @staticmethod
    def get_max_result(row):
        result = {}
        if row:
            for index, value in enumerate(row):
                if index == 0:
                    if value:
                        result["max"] = Decimal(value[0])
                    else:
                        result["max"] = 0
        return result
