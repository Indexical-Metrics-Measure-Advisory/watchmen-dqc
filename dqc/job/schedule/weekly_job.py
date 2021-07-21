import logging
from typing import List

import arrow

from dqc.common.utils.data_utils import build_collection_name
from dqc.config.config import settings
from dqc.job.schedule.daily_job import load_topic_list_without_raw_topic, execute_topic_rule
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.service.analysis.analysis_service import load_topic_rule_list_by_topic_id

log = logging.getLogger("app." + __name__)

def run():
    log.info("start global rule job at {}".format(arrow.now()))
    # site: dict = load_site_json()
    execute_topic_list = load_topic_list_without_raw_topic()
    execute_topic_rules(execute_topic_list)
    log.info("end global rule job at {}".format(arrow.now()))


def __find_execute_rule(rule_list:List[MonitorRule]):
    execute_rules = []
    for rule in rule_list:
        if rule.enabled :
            if rule.params is not None and rule.params.statisticalInterval =="weekly":
                execute_rules.append(rule)

    return execute_rules

def execute_topic_rules(execute_topic_list,site_info):
    for execute_topic in execute_topic_list:
        rule_list = load_topic_rule_list_by_topic_id(execute_topic["topicId"])
        enabled_rules = __find_execute_rule(rule_list)

        if enabled_rules:
            topic_name = build_collection_name(execute_topic["name"])
            log.info("check topic {}".format(topic_name))
            execute_topic_rule(enabled_rules, execute_topic,"weekly")