from storage.mongo.mongo_new_template import update_one_with_key
from storage.storage.storage_template import find_, insert_one

from dqc.common.simpleflake import get_next_id
from dqc.model.analysis.monitor_rule import MonitorRule

MONITOR_RULES = "monitor_rules"


def load_global_rule_list():
    return find_({"grade": "global"}, MonitorRule, MONITOR_RULES)


def load_topic_rule_list_by_topic_id(topic_id):
    return find_({"grade": "topic", "topicId": topic_id}, MonitorRule, MONITOR_RULES)


def create_monitor_rule(monitor_rule: MonitorRule):
    monitor_rule.uid = get_next_id()
    return insert_one(monitor_rule, MonitorRule, MONITOR_RULES)


def update_monitor_rule(monitor_rule: MonitorRule):
    return update_one_with_key(monitor_rule, MonitorRule, MONITOR_RULES, "uid")
