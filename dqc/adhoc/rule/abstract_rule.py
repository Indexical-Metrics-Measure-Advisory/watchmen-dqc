import datetime
from abc import abstractmethod, ABC
from decimal import Decimal
from typing import Optional

from model.model.topic.topic import Topic
from model.model.topic.factor import Factor

from dqc.adhoc.utils import get_from_to_date
from dqc.common.utils.data_utils import build_collection_name
from dqc.model.analysis.monitor_rule import MonitorRule


class AbstractRule(ABC):

    def __init__(self,
                 schema: str,
                 topic: Topic,
                 rule: MonitorRule,
                 factor: Optional[Factor] = None,
                 process_date: Optional[datetime.datetime] = None):
        self.schema = schema
        self.topic = topic
        self.rule = rule
        self.factor = factor
        self.process_date = process_date
        self.from_date = None
        self.to_date = None

    @abstractmethod
    def execute(self) -> bool:
        if self.rule.params:
            self.from_date, self.to_date = get_from_to_date(self.rule.params.statisticalInterval, self.process_date)
        else:
            self.from_date, self.to_date = get_from_to_date(None, self.process_date)
        return True

    def get_count_sql(self) -> str:
        sql = "SELECT count(*) as count " \
              "FROM {schema}.{table} " \
              "WHERE update_time_ between timestamp '{from_date}' and timestamp '{to_date}'".format(
            schema=self.schema,
            table=build_collection_name(self.topic.name),
            from_date=self.from_date.format('YYYY-MM-DD HH:mm:ss'),
            to_date=self.to_date.format('YYYY-MM-DD HH:mm:ss')
        )
        return sql

    def get_count_with_criterion_sql(self, criterion: str) -> str:
        sql = "{count_sql} and {criterion}".format(
            count_sql=self.get_count_sql(),
            criterion=criterion
        )
        return sql

    def get_cast_sql(self, data_type: str):
        sql = "SELECT CAST({field} AS {type}) " \
              "FROM {schema}.{table} " \
              "WHERE update_time_ between timestamp '{from_date}' and timestamp '{to_date}'".format(
            field=self.factor.name,
            type=data_type,
            schema=self.schema,
            table=build_collection_name(self.topic.name),
            from_date=self.from_date.format('YYYY-MM-DD HH:mm:ss'),
            to_date=self.to_date.format('YYYY-MM-DD HH:mm:ss')
        )
        return sql

    def get_field_sql(self) -> str:
        return self.get_field_sql_with_param(self.factor.name)

    def get_field_sql_with_param(self, factor_name):
        sql = "SELECT {field} " \
              "FROM {schema}.{table} " \
              "WHERE update_time_ between timestamp '{from_date}' and timestamp '{to_date}'".format(
            field=factor_name,
            schema=self.schema,
            table=build_collection_name(self.topic.name),
            from_date=self.from_date.format('YYYY-MM-DD HH:mm:ss'),
            to_date=self.to_date.format('YYYY-MM-DD HH:mm:ss')
        )
        return sql

    @staticmethod
    def get_criterion_sql(comparator: str, left: str, right: str) -> str:
        sql = "{left} {comparator} {right}".format(
            comparator=comparator,
            left=left,
            right=right,
        )
        return sql

    @staticmethod
    def get_count_result(row):
        result = {}
        if row:
            for index, value in enumerate(row):
                if index == 0:
                    result["count"] = value
        return result

    @staticmethod
    def get_factor(topic: Topic, factor_id: str):
        for factor in topic.factors:
            if factor.factorId == factor_id:
                return factor

    def check_value_not_in_range(self, value) -> bool:
        if self.rule.params:
            if self.rule.params.min and self.rule.params.max:
                range_min = int(self.rule.params.min)
                range_max = int(self.rule.params.max)
                result = range_min < value < range_max
                return not result
            else:
                return True
        else:
            return True
