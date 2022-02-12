import arrow
from model.model.topic.topic import Topic
from watchmen_boot.storage.model.data_source import DataSource

from dqc.config.config import settings
from dqc.model.analysis.monitor_rule_log import MonitorRuleLog
from dqc.model.analysis.rule_result_criteria import MonitorRuleLogCriteria
from dqc.presto.presto_client import get_connection
from dqc.rule.utils.factor_utils import find_factor_by_name, __convert_pandas_type


def __build_data_frame(rows, columns):
    if settings.DATAFRAME_TYPE == "pandas":
        import pandas as pd
        return pd.DataFrame(rows, columns=columns)
    elif settings.DATAFRAME_TYPE == "dask":
        import dask.dataframe as dd
        return dd.DataFrame(rows, columns=columns)


def query_topic_data_by_datetime(topic_name, from_datetime, to_datetime, topic: Topic = None,
                                 data_source: DataSource = None):
    topic_sql = "select * from {0} where update_time_ between timestamp '{1}' and  timestamp '{2}'".format(
        __build_topic_name(topic_name, data_source), from_datetime.format('YYYY-MM-DD'),
        to_datetime.format('YYYY-MM-DD'))
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(topic_sql)
    rows = cur.fetchall()
    columns = list([desc[0] for desc in cur.description])
    df = __build_data_frame(rows, columns)
    if topic is None:
        return df
    else:
        return convert_df_dtype(df, topic)


def convert_df_dtype(df, topic):
    dtype_dict = {}
    # print(df.columns)
    for column in df.columns:
        factor = find_factor_by_name(topic.factors, column)
        if factor is not None:
            dtype_dict[column] = __convert_pandas_type(factor.type)

    return df.astype(dtype_dict)


def __build_topic_name(topic_name, data_source: DataSource):
    return data_source.dataSourceCode + "." + data_source.name + "." + "topic_" + topic_name


def query_topic_data_count_by_datetime(topic, from_datetime, to_datetime, data_source):
    # topic_sql = "select count(*) from {0} ".format(__build_topic_name(topic["name"]))
    topic_sql = "select count(*) from {0} where update_time_ between timestamp '{1}' and  timestamp '{2}'".format(
        __build_topic_name(topic.name, data_source), from_datetime.format('YYYY-MM-DD'),
        to_datetime.format('YYYY-MM-DD'))

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(topic_sql)
    row = cur.fetchone()
    return row[0]


def generate_monitor_log_query(criteria: MonitorRuleLogCriteria, data_source, tenant_id):
    start = arrow.get(criteria.startDate)
    end = arrow.get(criteria.endDate)
    if criteria.ruleCode is None:
        return "select sum(count) as count,rulecode from {0} where tenant_id_ = '{1}' and update_time_ between " \
               "timestamp '{2}' and  timestamp '{3}' GROUP BY rulecode".format(
            __build_topic_name('rule_aggregate', data_source), tenant_id, start.format('YYYY-MM-DD HH:mm:ss ZZ'),
            end.format('YYYY-MM-DD HH:mm:ss ZZ'))
    elif criteria.topicId is None:
        return "select sum(count) as count,rulecode,topicid from {0} where  tenant_id_ = '{1}' and rulecode = '{2}' " \
               "and update_time_ between timestamp '{3}' and  timestamp '{4}' GROUP BY rulecode,topicid".format(
            __build_topic_name('rule_aggregate', data_source), tenant_id, criteria.ruleCode,
            start.format('YYYY-MM-DD HH:mm:ss ZZ'),
            end.format('YYYY-MM-DD HH:mm:ss ZZ'))
    elif criteria.factorId is None:
        return "select sum(count) as count,rulecode,topicid, factorid  from {0} where tenant_id_ = '{1}' and rulecode " \
               "= '{2}' and topicid = '{3}' and update_time_ between timestamp '{4}' and  timestamp '{5}' GROUP BY " \
               "rulecode,topicid,factorid".format(
            __build_topic_name('rule_aggregate', data_source), tenant_id, criteria.ruleCode, criteria.topicId,
            start.format('YYYY-MM-DD HH:mm:ss ZZ'),
            end.format('YYYY-MM-DD HH:mm:ss ZZ'))


def query_rule_results_by_datetime(criteria, data_source, tenant_id):
    topic_sql = generate_monitor_log_query(criteria, data_source, tenant_id)
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(topic_sql)
    rows = cur.fetchall()
    columns = list([desc[0] for desc in cur.description])
    df = __build_data_frame(rows, columns)
    rule_results = []
    for row in df.itertuples(index=True, name='Pandas'):
        rule_log = MonitorRuleLog()
        rule_log.count = float(row.count)
        if criteria.ruleCode:
            rule_log.topicId = row.topicid

        if criteria.topicId:
            rule_log.factorId = row.factorid

        rule_log.ruleCode = row.rulecode
        rule_results.append(rule_log)

    return rule_results
