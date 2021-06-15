import importlib

from dqc.common.constants import TOPIC_RULE, FACTOR_RULE

RULE_MODULE_PATH = "dqc.service.rule."

# FACTOR_RULE_PATH = "d"


def find_rule_func(rule_name, rule_type):
    if rule_type == TOPIC_RULE:
        rule_method = importlib.import_module(RULE_MODULE_PATH + rule_name)
        return rule_method.init()
    elif rule_type == FACTOR_RULE :
        rule_method = importlib.import_module(RULE_MODULE_PATH+ rule_name)
        return rule_method.init()


