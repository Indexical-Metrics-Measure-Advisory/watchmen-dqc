from pandas_profiling import ProfileReport

from dqc.common.constants import MONITOR_RULES
from dqc.common.simpleflake import get_next_id
from dqc.database.storage.storage_template import find_, list_all, find_one, insert_one, update_
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.service.query.index import query_topic_data_by_datetime


def load_global_rule_list():
    return find_({"grade": "global"}, MonitorRule, MONITOR_RULES)


def load_topic_rule_list_by_topic_id(topic_id):
    results = find_({"topicid": topic_id}, MonitorRule, MONITOR_RULES)
    return results


def load_monitor_rule_all():
    return list_all(MonitorRule, MONITOR_RULES)


def load_monitor_rule(monitor_rule):
    if monitor_rule.factorId is None:
        return find_one(
            {"and": [{"topicid": monitor_rule.topicId}, {"grade": monitor_rule.grade}, {"code": monitor_rule.code}]},
            MonitorRule, MONITOR_RULES)
    else:
        return find_one(
            {"and": [{"topicid": monitor_rule.topicId}, {"grade": monitor_rule.grade}, {"code": monitor_rule.code},
                     {"factorid": monitor_rule.factorId}]},
            MonitorRule, MONITOR_RULES)


def create_monitor_rule(monitor_rule: MonitorRule):
    monitor_rule.ruleId = get_next_id()
    return insert_one(monitor_rule, MonitorRule, MONITOR_RULES)


def update_monitor_rule(monitor_rule: MonitorRule):
    if monitor_rule.factorId is None:
        return update_(
            {"and": [{"topicid": monitor_rule.topicId}, {"grade": monitor_rule.grade}, {"code": monitor_rule.code}]},
            monitor_rule,
            MonitorRule, MONITOR_RULES)
    else:
        return update_(
            {"and": [{"topicid": monitor_rule.topicId}, {"grade": monitor_rule.grade}, {"code": monitor_rule.code},
                     {"factorid": monitor_rule.factorId}]}, monitor_rule,
            MonitorRule, MONITOR_RULES)


def topic_profile(topic, from_, to_):
    topic_name = topic["name"]
    df = query_topic_data_by_datetime(topic["name"], from_, to_, topic)
    if df.empty:
        return None
    else:
        profile = ProfileReport(df, title="{0} data profile report".format(topic_name), minimal=True)
        return profile.to_json()


