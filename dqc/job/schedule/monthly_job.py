import logging
from typing import List

from dqc.common.utils.data_utils import build_collection_name
from dqc.job.schedule.common import load_topic_list_without_raw_topic_by_tenant
from dqc.job.schedule.daily_job import execute_topic_rule
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.service.analysis.analysis_service import load_topic_rule_list_by_topic_id

log = logging.getLogger("app." + __name__)


def exec_rules(current_user):
    execute_topic_list = load_topic_list_without_raw_topic_by_tenant(current_user)
    execute_topic_rules(execute_topic_list, current_user)


def run():
    # remove schedule job in dqc instance
    pass
    """
    log.info("start global rule job at {}".format(arrow.now()))
    execute_topic_list = load_topic_list_without_raw_topic()
    execute_topic_rules(execute_topic_list)
    log.info("end global rule job at {}".format(arrow.now()))
    """


def __find_execute_rule(rule_list: List[MonitorRule]):
    execute_rules = []
    for rule in rule_list:
        if rule.enabled:
            if rule.params is not None and rule.params.statisticalInterval == "monthly":
                execute_rules.append(rule)

    return execute_rules


def execute_topic_rules(execute_topic_list, current_user):
    for execute_topic in execute_topic_list:
        rule_list = load_topic_rule_list_by_topic_id(execute_topic["topicId"])
        enabled_rules = __find_execute_rule(rule_list)

        if enabled_rules:
            topic_name = build_collection_name(execute_topic["name"])
            log.info("check topic {}".format(topic_name))
            execute_topic_rule(enabled_rules, execute_topic, "monthly", current_user)
