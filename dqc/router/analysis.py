from typing import List

from fastapi import APIRouter
from pydantic.main import BaseModel
from storage.mongo.mongo_new_template import update_one_with_key
from storage.storage.storage_template import insert_all, insert_one, find_

from dqc.common.constants import TOPIC_RULES, FACTOR_RULES
from dqc.common.simpleflake import get_next_id
from dqc.model.analysis.factor_rule import FactorRule
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.model.analysis.topic_rule import TopicRule

MONITOR_RULES = "monitor_rules"

router = APIRouter()


class MonitorRulesCriteria(BaseModel):
    grade: str = None
    topicId: str = None


## add rules to topic
@router.post("/dqc/topic/rules", tags=["admin"], response_model=List[TopicRule])
async def add_rule_to_topic(topic_rule_list: List[TopicRule]):
    for topic_rule in topic_rule_list:
        topic_rule.topicRuleId = get_next_id()
    return insert_all(topic_rule_list, TopicRule, TOPIC_RULES)


## add rules to factor
@router.post("/dqc/factor/rules", tags=["admin"], response_model=List[FactorRule])
async def add_rule_to_factor(factor_rule_list: List[FactorRule]):
    for factor_rule in factor_rule_list:
        factor_rule.factorRuleId = get_next_id()
    return insert_all(factor_rule_list, FactorRule, FACTOR_RULES)


@router.post("/dqc/topic/rule/update", tags=["admin"], response_model=TopicRule)
async def update_topic_rule(topic_rule: TopicRule):
    return update_one_with_key(topic_rule, TopicRule, TOPIC_RULES, "topicRuleId")


@router.post("/dqc/factor/rule/update", tags=["admin"], response_model=FactorRule)
async def update_factor_rule(factor_rule: FactorRule):
    return update_one_with_key(factor_rule, FactorRule, FACTOR_RULES, "factorRuleId")


@router.post("/dqc/monitor/rules", tags=["admin"], response_model=List[MonitorRule])
async def save_monitor_rule(rule_list: List[MonitorRule]):
    for monitor_rule in rule_list:
        if monitor_rule.uid is None:
            monitor_rule.uid  = get_next_id()
            insert_one(monitor_rule, MonitorRule, MONITOR_RULES)
        else:
            update_one_with_key(monitor_rule, MonitorRule, MONITOR_RULES, "uid")
    return rule_list



@router.post("/dqc/monitor/query", tags=["admin"], response_model=List[MonitorRule])
async def query_monitor_rules(criteria: MonitorRulesCriteria):
    if criteria.grade == "global":
        return find_({"grade":"global"},MonitorRule, MONITOR_RULES)
    else:
        return find_({"grade":"topic","topicId":criteria.topicId},MonitorRule, MONITOR_RULES)


