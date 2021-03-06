from typing import List

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from dqc.config.config import settings
from dqc.job.schedule import daily_job, weekly_job, monthly_job
from dqc.model.analysis.monitor_rule import MonitorRule
from dqc.service.analysis.analysis_service import load_monitor_rule_all


def group_by_statistical_interval(rules: List[MonitorRule]):
    group_by_result = {}
    for rule in rules:

        if rule.params is None or rule.params.statisticalInterval is None or rule.params.statisticalInterval == "daily":
            if "daily" in group_by_result:
                group_by_result["daily"].append(rule)
            else:
                group_by_result["daily"] = [rule]
        elif rule.params.statisticalInterval == "weekly":
            if "weekly" in group_by_result:
                group_by_result["weekly"].append(rule)
            else:
                group_by_result["weekly"] = [rule]
        elif rule.params.statisticalInterval == "monthly":
            if "monthly" in group_by_result:
                group_by_result["monthly"].append(rule)
            else:
                group_by_result["monthly"] = [rule]

    return group_by_result


def init_jobs():
    rules = load_monitor_rule_all()

    group_by_statistical_interval_results = group_by_statistical_interval(rules)

    if settings.JOB_FLAG:
        scheduler = AsyncIOScheduler()
        if "daily" in group_by_statistical_interval_results:
            print("init daily job ")
            scheduler.add_job(daily_job.run, settings.JOB_TRIGGER, day_of_week=settings.DAILY_DAY_OF_WEEK,
                              hour=settings.DAILY_HOURS, minute=settings.DAILY_MINUTES)
        if "weekly" in group_by_statistical_interval_results:
            print("init weekly job ")
            scheduler.add_job(weekly_job.run, settings.JOB_TRIGGER, day_of_week=settings.WEEKLY_DAY_OF_WEEK,
                              hour=settings.WEEKLY_HOURS,
                              minute=settings.WEEKLY_MINUTES)
        if "monthly" in group_by_statistical_interval_results:
            print("init monthly job ")
            scheduler.add_job(monthly_job.run, settings.JOB_TRIGGER, day=settings.MONTHLY_DAY,
                              hour=settings.MONTHLY_HOURS, minute=settings.MONTHLY_MINUTES)

        scheduler.start()
