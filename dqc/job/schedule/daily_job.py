import logging
from typing import List

import logging
from typing import List

import arrow

from dqc.common.utils.data_utils import build_collection_name
from dqc.config.config import settings
from dqc.job.schedule.common import load_topic_list_without_raw_topic, execute_topic_rule
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.service.analysis.analysis_service import load_global_rule_list, load_topic_rule_list_by_topic_id
from dqc.service.common.site_service import load_site_json

log = logging.getLogger("app." + __name__)


def __run_execute_rule(execute_topic_list, enabled_rules, site_info):
    for execute_topic in execute_topic_list:
        topic_name = build_collection_name(execute_topic["name"])
        log.info("check topic {}".format(topic_name))


        execute_topic_rule(enabled_rules, execute_topic, site_info, "daily")


def run():
    log.info("start global rule job at {}".format(arrow.now()))
    site: dict = load_site_json()
    execute_topic_list = load_topic_list_without_raw_topic(site[settings.WATCHMEN_NAME])
    execute_global_rule(execute_topic_list, site[settings.WATCHMEN_NAME])
    execute_topic_rules(execute_topic_list, site[settings.WATCHMEN_NAME])

    log.info("end global rule job at {}".format(arrow.now()))


def execute_topic_rules(execute_topic_list, site_info):
    for execute_topic in execute_topic_list:
        rule_list = load_topic_rule_list_by_topic_id(execute_topic["topicId"])
        enabled_rules = __find_execute_rule(rule_list)

        if enabled_rules:
            topic_name = build_collection_name(execute_topic["name"])
            log.info("check topic {}".format(topic_name))
            execute_topic_rule(enabled_rules, execute_topic, site_info, "daily")


def __find_execute_rule(rule_list: List[MonitorRule]):
    execute_rules = []
    for rule in rule_list:
        if rule.enabled:
            if rule.params is None:
                execute_rules.append(rule)
            elif rule.params is not None and rule.params.statisticalInterval is None:
                execute_rules.append(rule)
            elif rule.params is not None and rule.params.statisticalInterval == "daily":
                execute_rules.append(rule)

    return execute_rules


def execute_global_rule(execute_topic_list, site_info):
    rule_list: List[MonitorRule] = load_global_rule_list()
    enabled_rules = list(filter(lambda rule: rule.enabled, rule_list))
    ## execute global rule
    if enabled_rules:
        __run_execute_rule(execute_topic_list, enabled_rules, site_info)
