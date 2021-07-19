import importlib
import logging
import traceback

from trino.exceptions import TrinoUserError

from dqc.common.constants import TOPIC_RULE, FACTOR_RULE
from dqc.common.utils.date_utils import get_date_range
from dqc.model.analysis.rule_result import RuleExecuteResult
from dqc.sdk.admin.admin_sdk import load_all_topic_list
from dqc.sdk.common.common_sdk import import_instance, InstanceRequest
from dqc.service.query.index import query_topic_data_by_datetime

log = logging.getLogger("app." + __name__)

RULE_MODULE_PATH = "dqc.rule.basic."


def load_topic_list_without_raw_topic(site_info):
    topic_list = load_all_topic_list(site_info)
    # print(topic_list)
    filtered = filter(lambda topic: topic["type"] != "raw" and topic["kind"] == "business", topic_list)
    return list(filtered)


def get_topic_data(topic, from_date, to_date):
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


def trigger_rule(topic_result):
    return topic_result.result


def save_rule_result(rule_result_summary: RuleExecuteResult, site_info):
    if rule_result_summary and rule_result_summary.ruleType == TOPIC_RULE:
        if trigger_rule(rule_result_summary.topicResult):
            rule_result_summary.topicResult.result = bool(rule_result_summary.topicResult.result)
            import_instance(InstanceRequest(code="system_rule_result", data=rule_result_summary.topicResult), site_info)
    elif rule_result_summary and rule_result_summary.ruleType == FACTOR_RULE:
        factor_results = rule_result_summary.factorResult
        for factor_result in factor_results:
            if trigger_rule(factor_result):
                factor_result.result = bool(factor_result.result)
                import_instance(InstanceRequest(code="system_rule_result", data=factor_result), site_info)


def execute_topic_rule(enabled_rules, execute_topic, site_info, interval):
    try:
        start, end = get_date_range(interval)
        data_frame = get_topic_data(execute_topic, start, end)
        for enabled_rule in enabled_rules:

            log.info("run rule {}".format(enabled_rule.code))

            rule_func = find_rule_func(enabled_rule.code)
            if rule_func is not None:
                try:
                    rule_result = rule_func(data_frame, execute_topic, enabled_rule)
                    print(rule_result)
                    save_rule_result(rule_result, site_info)
                except Exception as e:
                    log.error(e)

                    log.error(traceback.format_exc())
            else:
                log.warning("rule not exists {}".format(enabled_rule.code))

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())