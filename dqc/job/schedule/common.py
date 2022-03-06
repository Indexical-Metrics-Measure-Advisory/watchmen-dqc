import importlib
import logging
import traceback

from model.model.common.user import User
from model.model.topic.topic import Topic
from trino.exceptions import TrinoUserError

from dqc.common.constants import TOPIC_RULE, FACTOR_RULE
from dqc.common.utils.data_utils import get_date_range
from dqc.model.analysis.rule_result import RuleExecuteResult
from dqc.sdk.admin.admin_sdk import load_all_topic_list, load_topic_list_by_tenant
from dqc.sdk.common.common_sdk import import_instance, InstanceRequest, get_datasource_by_id
from dqc.service.query.index import query_topic_data_by_datetime

log = logging.getLogger("app." + __name__)

RULE_MODULE_PATH = "dqc.rule.basic."


def load_topic_list_without_raw_topic(current_user):
    topic_list = load_all_topic_list(current_user)
    # print(topic_list)
    filtered = filter(lambda topic: topic["type"] != "raw" and topic.get("kind") == "business", topic_list)
    # print(filtered)
    return [Topic.parse_obj(result) for result in list(filtered)]


def get_topic_data(topic: Topic, from_date, to_date, current_user):
    try:
        if topic.dataSourceId:
            datasource = get_datasource_by_id(topic.dataSourceId, current_user)
            return query_topic_data_by_datetime(topic.name, from_date, to_date, topic, datasource)

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


def save_rule_result(rule_result_summary: RuleExecuteResult, current_user: User):
    if rule_result_summary and rule_result_summary.ruleType == TOPIC_RULE:
        if trigger_rule(rule_result_summary.topicResult):
            rule_result_summary.topicResult.result = bool(rule_result_summary.topicResult.result)
            import_instance(InstanceRequest(code="system_rule_result", data=rule_result_summary.topicResult,
                                            tenantId=rule_result_summary.tenantId), current_user)
    elif rule_result_summary and rule_result_summary.ruleType == FACTOR_RULE:
        factor_results = rule_result_summary.factorResult
        for factor_result in factor_results:
            if trigger_rule(factor_result):
                factor_result.result = bool(factor_result.result)
                import_instance(InstanceRequest(code="system_rule_result", data=factor_result,
                                                tenantId=rule_result_summary.tenantId), current_user)


def execute_topic_rule(enabled_rules, execute_topic, interval, current_user):
    try:
        start, end = get_date_range(interval)
        data_frame = get_topic_data(execute_topic, start, end, current_user)
        for enabled_rule in enabled_rules:
            log.info("run rule {}".format(enabled_rule.code))
            rule_func = find_rule_func(enabled_rule.code)
            if rule_func is not None:
                try:
                    rule_result = rule_func(data_frame, execute_topic, enabled_rule)
                    rule_result.tenantId = execute_topic.tenantId
                    save_rule_result(rule_result, current_user)
                except Exception as e:
                    log.error(e)
                    log.error(traceback.format_exc())
            else:
                log.warning("rule not exists {}".format(enabled_rule.code))

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())


def load_topic_list_without_raw_topic_by_tenant(current_user):
    topic_list = load_topic_list_by_tenant(current_user)
    filtered = filter(lambda topic: topic["type"] != "raw" and topic.get("kind") == "business", topic_list)
    return [Topic.parse_obj(result) for result in list(filtered)]
