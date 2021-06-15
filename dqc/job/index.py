from apscheduler.schedulers.asyncio import AsyncIOScheduler

from dqc.config.config import settings
from dqc.job.schedule.expect_rule_job import run_expect_rule_set_for_topic_data


def init_jobs():
    if settings.JOB_FLAG:
        scheduler = AsyncIOScheduler()
        scheduler.add_job(run_expect_rule_set_for_topic_data, 'interval', seconds=30)
        scheduler.start()
