import datetime
import logging
from abc import abstractmethod

from typing import Optional, Dict, Union

from model.model.topic.topic import Topic
from model.model.topic.factor import Factor

from dqc.adhoc.rule.abstract_rule import AbstractRule
from dqc.adhoc.utils import build_data_frame, convert_pandas_type
from dqc.model.analysis.monitor_rule import MonitorRule
import pandas as pd
import dask.dataframe as dd

log = logging.getLogger("app." + __name__)


class AbstractFactorUsePandas(AbstractRule):

    def __init__(self, schema: str, topic: Topic, rule: MonitorRule, factor: Optional[Factor] = None,
                 process_date: Optional[datetime.datetime] = None):
        super().__init__(schema, topic, rule, factor, process_date)

    @abstractmethod
    def execute(self) -> bool:
        super().execute()

    def get_factor_data_sql(self) -> str:
        return self.get_field_sql()

    def get_data_frame(self, cursor, rows) -> Union[pd.DataFrame, dd.DataFrame]:
        if rows:
            columns = list([desc[0] for desc in cursor.description])
            data_type = self.get_factor_data_type()
            data_frame = build_data_frame(rows, columns, data_type)
            return data_frame

    def get_factor_data_type(self) -> Dict:
        result = {self.factor.name.lower(): convert_pandas_type(self.factor.type)}
        return result



