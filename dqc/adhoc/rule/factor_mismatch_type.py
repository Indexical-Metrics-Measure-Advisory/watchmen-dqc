import datetime
import logging
from typing import Optional

from model.model.topic.factor import Factor
from model.model.topic.topic import Topic
from trino.exceptions import TrinoUserError

from dqc.adhoc.constants import FactorType
from dqc.adhoc.rule.abstract_rule import AbstractRule
from dqc.common.utils.data_utils import build_collection_name
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.presto.presto_client import get_connection

log = logging.getLogger("app." + __name__)


class FactorMismatchType(AbstractRule):

    def __init__(self, schema: str, topic: Topic, rule: MonitorRule, factor: Optional[Factor] = None,
                 process_date: Optional[datetime.datetime] = None):
        super().__init__(schema, topic, rule, factor, process_date)

    def execute(self) -> bool:
        super().execute()
        if self.factor.type == FactorType.UNSIGNED:
            match_result = self.match_unsigned()
            result = match_result["count"] == 0
            return result
        elif self.factor.type == FactorType.DATE or self.factor.type == FactorType.DATETIME:
            result = self.match_date()
            return result
        elif self.factor.type == FactorType.MINUTE:
            match_result = self.match_minute()
            result = match_result["count"] == 0
            return result
        elif self.factor.type == FactorType.YEAR:
            match_result = self.match_year()
            result = match_result["count"] == 0
            return result
        elif self.factor.type == FactorType.HALF_YEAR:
            match_result = self.match_half_year()
            result = match_result["count"] == 0
            return result
        elif self.factor.type == FactorType.SEQUENCE:
            result = self.match_sequence()
            return result
        elif self.factor.type == FactorType.NUMBER:
            result = self.match_number()
            return result
        elif self.factor.type == FactorType.QUARTER:
            match_result = self.match_quarter()
            result = match_result["count"] == 0
            return result
        elif self.factor.type == FactorType.DAY_OF_MONTH:
            match_result = self.match_day_of_month()
            result = match_result["count"] == 0
            return result
        else:
            return True

    def match_unsigned_sql(self) -> str:
        criterion_sql = self.get_criterion_sql("<", self.factor.name, 0)
        query_sql = self.get_count_with_criterion_sql(criterion_sql)
        return query_sql

    def match_unsigned(self):
        sql = self.match_unsigned_sql()
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

    def match_date_sql(self):
        sql = "SELECT CAST({field} AS date) " \
              "FROM {schema}.{table} " \
              "WHERE update_time_ between timestamp '{from_date}' and timestamp '{to_date}'".format(
            field=self.factor.name.lower(),
            schema=self.schema,
            table=build_collection_name(self.topic.name),
            from_date=self.from_date.format('YYYY-MM-DD HH:mm:ss'),
            to_date=self.to_date.format('YYYY-MM-DD HH:mm:ss')
        )
        return sql

    def match_date(self) -> bool:
        sql = self.match_date_sql()
        log.info(sql)
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql)
        try:
            cur.fetchall()
        except TrinoUserError:
            return False
        else:
            return True

    def match_minute_sql(self, from_, to_) -> str:
        criterion_sql = "({field} < {from_} or {field} > {to_})".format(
            field=self.factor.name,
            from_=from_,
            to_=to_
        )
        query_sql = self.get_count_with_criterion_sql(criterion_sql)
        return query_sql

    def match_minute(self) -> bool:
        sql = self.match_minute_sql(0, 59)
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        result = self.get_count_result(row)
        return result

    def match_year_sql(self) -> str:
        criterion_sql = "({field} NOT BETWEEN 1000 AND 3000)".format(
            field=self.factor.name
        )
        query_sql = self.get_count_with_criterion_sql(criterion_sql)
        return query_sql

    def match_year(self) -> bool:
        sql = self.match_year_sql()
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        result = self.get_count_result(row)
        return result

    def match_half_year_sql(self) -> str:
        criterion_sql = "({field} !=1 and {field} !=2 )".format(
            field=self.factor.name
        )
        query_sql = self.get_count_with_criterion_sql(criterion_sql)
        return query_sql

    def match_half_year(self) -> bool:
        sql = self.match_half_year_sql()
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        result = self.get_count_result(row)
        return result

    def match_sequence_sql(self) -> str:
        return self.get_cast_sql("BIGINT")

    def match_sequence(self) -> bool:
        sql = self.match_sequence_sql()
        log.info(sql)
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql)
        try:
            rows = cur.fetchall()
        except TrinoUserError:
            return False
        else:
            return True

    def match_number_sql(self) -> str:
        return self.get_cast_sql("DECIMAL")

    def match_number(self) -> bool:
        sql = self.match_number_sql()
        log.info(sql)
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql)
        try:
            rows = cur.fetchall()
        except TrinoUserError:
            return False
        else:
            return True

    def match_quarter_sql(self) -> str:
        criterion_sql = "({field} NOT BETWEEN 1 AND 4)".format(
            field=self.factor.name
        )
        query_sql = self.get_count_with_criterion_sql(criterion_sql)
        return query_sql

    def match_quarter(self) -> bool:
        sql = self.match_quarter_sql()
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        result = self.get_count_result(row)
        return result

    def match_day_of_month_sql(self) -> str:
        criterion_sql = "({field} NOT BETWEEN 1 AND 31)".format(
            field=self.factor.name
        )
        query_sql = self.get_count_with_criterion_sql(criterion_sql)
        return query_sql

    def match_day_of_month(self) -> bool:
        sql = self.match_day_of_month_sql()
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        result = self.get_count_result(row)
        return result
