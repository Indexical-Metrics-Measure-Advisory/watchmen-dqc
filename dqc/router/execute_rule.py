import datetime
from typing import Optional

from fastapi import APIRouter, Depends

from dqc.adhoc.rule_service import RuleService
from dqc.common import deps

router = APIRouter()


@router.get("/dqc/topics/rules", tags=["execute_rule"])
def run_topics_rules(frequency: str, current_user=Depends(deps.get_current_user)):
    if frequency == "monthly":
        from dqc.job.schedule.monthly_job import exec_rules
        exec_rules(current_user)
    elif frequency == "weekly":
        from dqc.job.schedule.weekly_job import exec_rules
        exec_rules(current_user)
    elif frequency == "daily":
        from dqc.job.schedule.daily_job import exec_rules
        exec_rules(current_user)


@router.get("/dqc/topic/rules", tags=["execute_rule"])
def run_topic_rules(topic_name: str,
                    process_date: Optional[datetime.date] = None,
                    current_user=Depends(deps.get_current_user)):
    rs_ = RuleService(topic_name, current_user, process_date)
    rs_.rules_execution()
