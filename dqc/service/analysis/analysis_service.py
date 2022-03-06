import logging

from pandas_profiling import ProfileReport
from storage.storage import storage_template

from dqc.common.constants import MONITOR_RULES

from watchmen_boot.guid.snowflake import get_surrogate_key
from dqc.database.find_storage_template import find_storage_template
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.service.query.index import query_topic_data_by_datetime

log = logging.getLogger("app." + __name__)

storage_template = find_storage_template()


def load_global_rule_list():
    return storage_template.find_({"grade": "global"}, MonitorRule, MONITOR_RULES)


def load_topic_rule_list_by_topic_id(topic_id,current_user):
    results = storage_template.find_(
        {"and": [{"topicid":topic_id},
                 {"tenantid": current_user.tenantId}]}
        , MonitorRule, MONITOR_RULES)
    return results


def load_monitor_rule_all():
    return storage_template.list_all(MonitorRule, MONITOR_RULES)


def load_monitor_rule(monitor_rule, current_user=None):
    if monitor_rule.factorId is None:
        return storage_template.find_one(
            {"and": [{"topicid": monitor_rule.topicId}, {"grade": monitor_rule.grade}, {"code": monitor_rule.code},
                     {"tenantid": current_user.tenantId}]},
            MonitorRule, MONITOR_RULES)
    else:
        return storage_template.find_one(
            {"and": [{"topicid": monitor_rule.topicId}, {"grade": monitor_rule.grade}, {"code": monitor_rule.code},
                     {"factorid": monitor_rule.factorId}, {"tenantid": current_user.tenantId}]},
            MonitorRule, MONITOR_RULES)


def create_monitor_rule(monitor_rule: MonitorRule, current_user):
    monitor_rule.ruleId = get_surrogate_key()
    monitor_rule.tenantId = current_user.tenantId
    return storage_template.insert_one(monitor_rule, MonitorRule, MONITOR_RULES)


def update_monitor_rule(monitor_rule: MonitorRule, current_user):
    monitor_rule.tenantId = current_user.tenantId
    if monitor_rule.factorId is None:
        return storage_template.update_one_first(
            {"and": [{"topicid": monitor_rule.topicId}, {"grade": monitor_rule.grade}, {"code": monitor_rule.code},
                     {"tenantid": current_user.tenantId}]},
            monitor_rule,
            MonitorRule, MONITOR_RULES)
    else:
        return storage_template.update_one_first(
            {"and": [{"topicid": monitor_rule.topicId}, {"grade": monitor_rule.grade}, {"code": monitor_rule.code},
                     {"factorid": monitor_rule.factorId}, {"tenantid": current_user.tenantId}]}, monitor_rule,
            MonitorRule, MONITOR_RULES)


def topic_profile(topic, from_, to_, data_source):
    topic_name = topic.name
    df = query_topic_data_by_datetime(topic.name, from_, to_, topic, data_source)
    if df.empty or len(df.index) == 1:
        return None
    else:
        log.info("memory_usage {0} bytes".format(df.memory_usage(deep=True).sum()))
        profile = ProfileReport(df, title="{0} data profile report".format(topic_name), minimal=True)
        json = profile.to_json()
        return json
