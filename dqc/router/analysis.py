from typing import List

from fastapi import APIRouter
from pydantic.main import BaseModel

from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.service.analysis.analysis_service import load_global_rule_list, load_topic_rule_list_by_topic_id, \
    create_monitor_rule, update_monitor_rule

router = APIRouter()


class MonitorRulesCriteria(BaseModel):
    grade: str = None
    topicId: str = None


class MonitorRuleRequest(BaseModel):
    criteria: MonitorRulesCriteria

#
# ## add rules to topic
# @router.post("/dqc/topic/rules", tags=["admin"], response_model=List[TopicRule])
# async def add_rule_to_topic(topic_rule_list: List[TopicRule]):
#     for topic_rule in topic_rule_list:
#         topic_rule.topicRuleId = get_next_id()
#     return insert_all(topic_rule_list, TopicRule, TOPIC_RULES)
#
#
# ## add rules to factor
# @router.post("/dqc/factor/rules", tags=["admin"], response_model=List[FactorRule])
# async def add_rule_to_factor(factor_rule_list: List[FactorRule]):
#     for factor_rule in factor_rule_list:
#         factor_rule.factorRuleId = get_next_id()
#     return insert_all(factor_rule_list, FactorRule, FACTOR_RULES)
#
#
# @router.post("/dqc/topic/rule/update", tags=["admin"], response_model=TopicRule)
# async def update_topic_rule(topic_rule: TopicRule):
#     return update_one_with_key(topic_rule, TopicRule, TOPIC_RULES, "topicRuleId")
#
#
# @router.post("/dqc/factor/rule/update", tags=["admin"], response_model=FactorRule)
# async def update_factor_rule(factor_rule: FactorRule):
#     return update_one_with_key(factor_rule, FactorRule, FACTOR_RULES, "factorRuleId")
#


@router.post("/dqc/monitor/rules", tags=["admin"], response_model=List[MonitorRule])
async def save_monitor_rule(rule_list: List[MonitorRule]):
    for monitor_rule in rule_list:
        if monitor_rule.uid is None:
            create_monitor_rule(monitor_rule)
        else:
            update_monitor_rule(monitor_rule)
    return rule_list


@router.post("/dqc/monitor/query", tags=["admin"], response_model=List[MonitorRule])
async def query_monitor_rules(req: MonitorRuleRequest):
    criteria = req.criteria
    if criteria.grade == "global":
        return load_global_rule_list()
    else:
        return load_topic_rule_list_by_topic_id(criteria.topicId)
