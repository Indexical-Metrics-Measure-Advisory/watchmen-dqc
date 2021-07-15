import arrow
import pandas as pd

from dqc.model.analysis.monitor_rule_log import MonitorRuleLog
from dqc.model.analysis.rule_result_criteria import MonitorRuleLogCriteria
from dqc.presto.presto_client import get_connection
from dqc.rule.utils.factor_utils import find_factor_by_name, __convert_pandas_type


def query_topic_data_by_datetime(topic_name, from_datetime, to_datetime, topic=None):
    topic_sql = "select * from {0} where update_time_ between timestamp '{1}' and  timestamp '{2}'".format(
        __build_topic_name(topic_name), from_datetime.format('YYYY-MM-DD'), to_datetime.format('YYYY-MM-DD'))
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(topic_sql)
    rows = cur.fetchall()
    columns = list([desc[0] for desc in cur.description])
    df = pd.DataFrame(rows, columns=columns)
    if topic is None:
        return df
    else:
        return convert_df_dtype(df, topic)


def convert_df_dtype(df, topic):
    dtype_dict = {}
    # print(df.columns)
    for column in df.columns:
        factor = find_factor_by_name(topic["factors"], column)
        if factor is not None:
            dtype_dict[column] = __convert_pandas_type(factor["type"])

    return df.astype(dtype_dict)


def __build_topic_name(topic_name):
    return "topic_" + topic_name


def query_topic_data_count_by_datetime(topic, from_datetime, to_datetime):
    topic_sql = "select count(*) from {0} ".format(__build_topic_name(topic["name"]))
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(topic_sql)
    row = cur.fetchone()
    return row[0]


def generate_monitor_log_query(criteria: MonitorRuleLogCriteria):
    start = arrow.get(criteria.startDate)
    end = arrow.get(criteria.endDate)
    if criteria.ruleCode is None:
        return "select sum(count) as count,rulecode from {0} where update_time_ between timestamp '{1}' and  timestamp '{2}' GROUP BY rulecode".format(
            'topic_rule_aggregate', start.format('YYYY-MM-DD HH:mm:ss ZZ'),
            end.format('YYYY-MM-DD HH:mm:ss ZZ'))
    elif criteria.topicId is None:
        return "select sum(count) as count,rulecode,topicid from {0} where rulecode = '{1}' and update_time_ between timestamp '{2}' and  timestamp '{3}' GROUP BY rulecode,topicid".format(
            'topic_rule_aggregate', criteria.ruleCode, start.format('YYYY-MM-DD HH:mm:ss ZZ'),
            end.format('YYYY-MM-DD HH:mm:ss ZZ'))
    elif criteria.factorId is None:
        return "select sum(count) as count,rulecode,topicid, factorid  from {0} where rulecode = '{1}' and topicid = '{2}' and update_time_ between timestamp '{3}' and  timestamp '{4}' GROUP BY rulecode,topicid,factorid".format(
            'topic_rule_aggregate', criteria.ruleCode, criteria.topicId, start.format('YYYY-MM-DD HH:mm:ss ZZ'),
            end.format('YYYY-MM-DD HH:mm:ss ZZ'))


def query_rule_results_by_datetime(criteria):
    topic_sql = generate_monitor_log_query(criteria)

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(topic_sql)
    rows = cur.fetchall()
    columns = list([desc[0] for desc in cur.description])
    df = pd.DataFrame(rows, columns=columns)
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
