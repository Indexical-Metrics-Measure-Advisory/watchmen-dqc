import datetime
import logging
from typing import Optional, Dict

from model.model.topic.factor import Factor
from model.model.topic.topic import Topic

from dqc.adhoc.rule.abstract_rule import AbstractRule
from dqc.adhoc.utils import build_data_frame, convert_pandas_type, df_series_is_str
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.presto.presto_client import get_connection

log = logging.getLogger("app." + __name__)


class FactorUseCast(AbstractRule):

    def __init__(self, schema: str, topic: Topic, rule: MonitorRule, factor: Optional[Factor] = None,
                 process_date: Optional[datetime.datetime] = None):
        super().__init__(schema, topic, rule, factor, process_date)
        self.schema = schema
        self.topic = topic
        self.rule = rule
        self.factor = factor
        self.process_date = process_date
        self.from_date = None
        self.to_date = None

    def execute(self) -> bool:
        super().execute()
        return self.match_use_cast()

    def get_factor_use_cast_sql(self) -> str:
        return self.get_field_sql()

    def match_use_cast(self) -> bool:
        sql = self.get_factor_use_cast_sql()
        log.info("factor use cast: {sql}".format(sql=sql))
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        if rows:
            columns = list([desc[0] for desc in cur.description])
            data_type = self.get_factor_data_type()
            data_frame = build_data_frame(rows, columns, data_type)
            df_series = data_frame[self.factor.name.lower()]
            if df_series_is_str(df_series):
                result = [df_series.str.isnumeric().all(), df_series.str.isdecimal().all()]
                return True in result
            else:
                return False
        else:
            return False

    def get_factor_data_type(self) -> Dict:
        result = {self.factor.name.lower(): convert_pandas_type(self.factor.type)}
        return result
