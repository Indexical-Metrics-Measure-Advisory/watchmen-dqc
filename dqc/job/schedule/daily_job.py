import importlib
import logging
from typing import List

import arrow
from trino.exceptions import TrinoUserError

from dqc.common.constants import TOPIC_RULE, FACTOR_RULE
from dqc.common.utils.date_utils import get_date_range
from dqc.config.config import settings
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.model.analysis.rule_result import RuleExecuteResult
from dqc.sdk.admin.admin_sdk import load_all_topic_list
from dqc.sdk.common.common_sdk import import_instance, InstanceRequest
from dqc.service.analysis.analysis_service import load_global_rule_list
from dqc.service.common.site_service import load_site_json
from dqc.service.query.index import query_topic_data_by_datetime

log = logging.getLogger("app." + __name__)

RULE_MODULE_PATH = "dqc.rule.basic."


def load_topic_list_without_raw_topic(site_info):
    topic_list = load_all_topic_list(site_info)
    # print(topic_list)
    filtered = filter(lambda topic: topic["type"] != "raw" and topic["kind"] == "business", topic_list)
    return list(filtered)


def __build_topic_name(topic_name):
    return "topic_" + topic_name


def __get_topic_data(topic, from_date, to_date):
    try:
        return query_topic_data_by_datetime(topic["name"], from_date, to_date, topic)
    except TrinoUserError as error:
        log.error(error)
        if error.error_name == "TABLE_NOT_FOUND":
            return None
        # raise error


def find_rule_func(rule_code, rule_type=None):
    rule_name = rule_code.replace("-", "_")
    try:
        if rule_type == TOPIC_RULE:
            rule_method = importlib.import_module(RULE_MODULE_PATH + rule_name)
            return rule_method.init()
        elif rule_type == FACTOR_RULE:
            rule_method = importlib.import_module(RULE_MODULE_PATH + rule_name)
            return rule_method.init()
        else:
            rule_method = importlib.import_module(RULE_MODULE_PATH + rule_name)
            return rule_method.init()
    except Exception as e:
        log.warning(e)


def trigger_rule(topicResult):
    return topicResult.result


def __save_rule_result(rule_result_summary: RuleExecuteResult, site_info):
    if rule_result_summary and rule_result_summary.ruleType == TOPIC_RULE:
        if trigger_rule(rule_result_summary.topicResult):
            import_instance(InstanceRequest(code="system_rule_result", data=rule_result_summary.topicResult), site_info)
    elif rule_result_summary and rule_result_summary.ruleType == FACTOR_RULE:
        factor_results = rule_result_summary.factorResult
        for factor_result in factor_results:
            if trigger_rule(factor_result):
                import_instance(InstanceRequest(code="system_rule_result", data=factor_result), site_info)


def execute_topic_rule(enabled_rules, execute_topic, site_info):
    try:
        ##TODO date time

        start, end = get_date_range("daily")
        data_frame = __get_topic_data(execute_topic, start, end)

        for enabled_rule in enabled_rules:

            log.info("run rule {}".format(enabled_rule.code))
            rule_func = find_rule_func(enabled_rule.code)
            if rule_func is not None:
                try:
                    rule_result = rule_func(data_frame, execute_topic, enabled_rule)
                    __save_rule_result(rule_result, site_info)
                except Exception as e:
                    log.error(e)
                    log.error("func execute error: {}".format(enabled_rule.code))
            else:
                log.warning("rule not exists {}".format(enabled_rule.code))

    except Exception as e:
        log.error(e)


def __run_execute_rule(execute_topic_list, enabled_rules, site_info):
    for execute_topic in execute_topic_list:
        topic_name = __build_topic_name(execute_topic["name"])
        log.info("check topic {}".format(topic_name))
        execute_topic_rule(enabled_rules, execute_topic, site_info)


def run():
    log.info("start global rule job at {}".format(arrow.now()))
    site: dict = load_site_json()
    execute_topic_list = load_topic_list_without_raw_topic(site[settings.WATCHMEN_NAME])
    execute_global_rule(execute_topic_list, site[settings.WATCHMEN_NAME])

    log.info("end global rule job at {}".format(arrow.now()))


def execute_global_rule(execute_topic_list, site_info):
    rule_list: List[MonitorRule] = load_global_rule_list()
    enabled_rules = list(filter(lambda rule: rule.enabled, rule_list))
    ## execute global rule
    if enabled_rules:
        __run_execute_rule(execute_topic_list, enabled_rules, site_info)
