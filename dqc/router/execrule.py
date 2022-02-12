from fastapi import APIRouter, Depends
from dqc.common import deps

router = APIRouter()


@router.get("/dqc/topics/rules", tags=["execrule"])
def run_topic_rules(frequency: str, current_user=Depends(deps.get_current_user)):
    if frequency == "monthly":
        from dqc.job.schedule.monthly_job import exec_rules
        exec_rules(current_user)
    elif frequency == "weekly":
        from dqc.job.schedule.weekly_job import exec_rules
        exec_rules(current_user)
    elif frequency == "daily":
        from dqc.job.schedule.daily_job import exec_rules
        exec_rules(current_user)
