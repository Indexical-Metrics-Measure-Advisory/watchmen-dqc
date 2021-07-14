from storage.storage.storage_template import find_, insert_one, update_, find_one, list_all

from dqc.common.simpleflake import get_next_id
from dqc.model.analysis.monitor_rule import MonitorRule

MONITOR_RULES = "monitor_rules"


def load_global_rule_list():
    return find_({"grade": "global"}, MonitorRule, MONITOR_RULES)


def load_topic_rule_list_by_topic_id(topic_id):
    results = find_({"topicId": topic_id}, MonitorRule, MONITOR_RULES)
    return results


def load_monitor_rule_all():
    return list_all(MonitorRule, MONITOR_RULES)


def load_monitor_rule(monitor_rule):
    if monitor_rule.factorId is None:
        return find_one({"and": [{"topicId": monitor_rule.topicId}, {"code": monitor_rule.code}]},
                        MonitorRule, MONITOR_RULES)
    else:
        return find_one({"and": [{"topicId": monitor_rule.topicId}, {"code": monitor_rule.code},
                                 {"factorId": monitor_rule.factorId}]},
                        MonitorRule, MONITOR_RULES)


def create_monitor_rule(monitor_rule: MonitorRule):
    monitor_rule.uid = get_next_id()
    return insert_one(monitor_rule, MonitorRule, MONITOR_RULES)


def update_monitor_rule(monitor_rule: MonitorRule):
    if monitor_rule.factorId is None:
        print("topic ")
        return update_({"and": [{"topicId": monitor_rule.topicId}, {"code": monitor_rule.code}]}, monitor_rule,
                       MonitorRule, MONITOR_RULES)
    else:
        print("factor ")
        return update_({"and": [{"topicId": monitor_rule.topicId}, {"code": monitor_rule.code},
                                {"factorId": monitor_rule.factorId}]}, monitor_rule,
                       MonitorRule, MONITOR_RULES)
