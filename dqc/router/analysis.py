from typing import List

from fastapi import APIRouter
from pydantic.main import BaseModel

from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.service.analysis.analysis_service import load_global_rule_list, load_topic_rule_list_by_topic_id, \
    create_monitor_rule, update_monitor_rule, load_monitor_rule

router = APIRouter()


class MonitorRulesCriteria(BaseModel):
    grade: str = None
    topicId: str = None


class MonitorRuleRequest(BaseModel):
    criteria: MonitorRulesCriteria




@router.post("/dqc/monitor/rules", tags=["admin"], response_model=List[MonitorRule])
async def save_monitor_rule(rule_list: List[MonitorRule]):
    for monitor_rule in rule_list:
        result = load_monitor_rule(monitor_rule)
        if result is None:
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
