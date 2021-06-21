import logging
from typing import List

import arrow
from trino.exceptions import TrinoUserError

from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.rule.index import find_rule_func
from dqc.sdk.admin.admin_sdk import load_all_topic_list
from dqc.service.analysis.analysis_service import load_global_rule_list
from dqc.service.common.site_service import load_site_json
from dqc.service.query.index import query_topic_data_by_datetime

log = logging.getLogger("app." + __name__)


def __load_global_rules() -> List[MonitorRule]:
    return load_global_rule_list()


def __load_topic_list_without_raw_topic(site_info):
    topic_list = load_all_topic_list(site_info)
    # print(topic_list)
    filtered = filter(lambda topic: topic["type"] != "raw" and topic["kind"] == "business", topic_list)
    return list(filtered)


def __build_topic_name(topic_name):
    return "topic_" + topic_name


def __save_rule_result(result_type, rule_result_summary):
    print("rule_result_summary", rule_result_summary)


def __get_topic_data(topic_name, from_date, to_date):
    try:
        return query_topic_data_by_datetime(topic_name, None, None)
    except TrinoUserError as error:
        if error.error_name == "TABLE_NOT_FOUND":
            return None
        raise error


def __run_execute_rule(execute_topic_list, enabled_rules):
    for execute_topic in execute_topic_list:
        topic_name = __build_topic_name(execute_topic["name"])
        log.info("check topic {}".format(topic_name))
        execute_topic_rule(enabled_rules, execute_topic, topic_name)


def execute_topic_rule(enabled_rules, execute_topic, topic_name):
    try:
        ##TODO date time
        data_frame = __get_topic_data(topic_name, None, None)
        for enabled_rule in enabled_rules:
            log.info("run rule {}".format(enabled_rule.code))
            rule_func = find_rule_func(enabled_rule.code)
            if rule_func is not None:
                result_type, rule_result = rule_func(data_frame, execute_topic, enabled_rule)
                __save_rule_result(result_type, rule_result)
            else:
                log.warning("rule not exists {}".format(enabled_rule.code))

    except Exception as e:
        log.error(e)


def run():
    log.info("start global rule job at {}".format(arrow.now()))
    site: dict = load_site_json()
    rule_list: List[MonitorRule] = __load_global_rules()
    enabled_rules = list(filter(lambda rule: rule.enabled, rule_list))
    if enabled_rules:
        for key, site_info in site.items():
            execute_topic_list = __load_topic_list_without_raw_topic(site_info)
            __run_execute_rule(execute_topic_list, enabled_rules)

    log.info("end global rule job at {}".format(arrow.now()))
