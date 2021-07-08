import importlib
import logging

from dqc.common.constants import TOPIC_RULE, FACTOR_RULE

RULE_MODULE_PATH = "dqc.rule.basic"


log = logging.getLogger("app." + __name__)


def find_rule_func(rule_code, rule_type=None):
    rule_name = rule_code.replace("-","_")
    try:
        if rule_type == TOPIC_RULE:
            rule_method = importlib.import_module(RULE_MODULE_PATH + rule_name)
            return rule_method.init()
        elif rule_type == FACTOR_RULE:
            rule_method = importlib.import_module(RULE_MODULE_PATH+ rule_name)
            return rule_method.init()
        else:
            rule_method = importlib.import_module(RULE_MODULE_PATH + rule_name)
            return rule_method.init()
    except Exception as e:
        log.warning(e)




