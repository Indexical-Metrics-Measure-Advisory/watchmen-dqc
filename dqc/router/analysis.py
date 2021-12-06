import json
from datetime import datetime
from typing import List

import arrow
from fastapi import APIRouter, Depends
from model.model.common.user import User
from model.model.topic.topic import Topic
from pydantic.main import BaseModel
from storage.model.data_source import DataSource

from dqc.common import deps
from dqc.common.simpleflake import get_next_id
from dqc.common.utils.data_utils import get_date_range_with_end_date
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.model.analysis.monitor_rule_log import MonitorRuleLog
from dqc.model.analysis.rule_result_criteria import MonitorRuleLogRequest
from dqc.sdk.admin.admin_sdk import get_topic_by_id, load_topic_by_name_and_tenant
from dqc.sdk.common.common_sdk import get_datasource_by_id
from dqc.service.analysis.analysis_service import load_global_rule_list, load_topic_rule_list_by_topic_id, \
    create_monitor_rule, update_monitor_rule, load_monitor_rule, topic_profile
from dqc.service.query.index import query_rule_results_by_datetime

# import re

router = APIRouter()

json_constant_map = {
    '-Infinity': float('-Infinity'),
    'Infinity': float('Infinity'),
    'NaN': None,
}


class MonitorRulesCriteria(BaseModel):
    grade: str = None
    topicId: str = None


class MonitorRuleRequest(BaseModel):
    criteria: MonitorRulesCriteria


class TopicProfileRequest(BaseModel):
    topicId: str = None
    fromDate: datetime = None
    toDate: datetime = None


@router.post("/dqc/monitor/rules", tags=["admin"], response_model=List[MonitorRule])
async def save_monitor_rule(rule_list: List[MonitorRule], current_user=Depends(deps.get_current_user)):
    for monitor_rule in rule_list:
        if monitor_rule.ruleId is None:
            monitor_rule.ruleId = get_next_id()
        result = load_monitor_rule(monitor_rule,current_user)
        if result is None:
            create_monitor_rule(monitor_rule, current_user)
        else:
            update_monitor_rule(monitor_rule, current_user)
    return rule_list


@router.post("/dqc/monitor/query", tags=["admin"], response_model=List[MonitorRule])
async def query_monitor_rules(req: MonitorRuleRequest, current_user=Depends(deps.get_current_user)):
    criteria = req.criteria
    if criteria.grade == "global":
        return load_global_rule_list()
    else:
        return load_topic_rule_list_by_topic_id(criteria.topicId)


@router.post("/dqc/rule/result/query", tags=["admin"], response_model=List[MonitorRuleLog])
async def query_rule_results(req: MonitorRuleLogRequest, current_user:User=Depends(deps.get_current_user)):
    topic:Topic = load_topic_by_name_and_tenant("rule_aggregate",current_user.tenantId)
    data_source:DataSource = get_datasource_by_id(topic.dataSourceId)
    results = query_rule_results_by_datetime(req.criteria,data_source,current_user.tenantId)
    return results


@router.get("/dqc/topic/profile", tags=["analytics"])
def generate_topic_profile(topic_id: str, date: str, current_user=Depends(deps.get_current_user)):
    query_date = arrow.get(date)
    topic = get_topic_by_id(topic_id)
    data_source: DataSource = get_datasource_by_id(topic.dataSourceId)
    from_, to_ = get_date_range_with_end_date("daily", query_date)
    data = topic_profile(topic, from_, to_,data_source)
    if data:
        return json.loads(
            data,
            parse_constant=lambda constant: json_constant_map[constant],
        )
    else:
        return None
