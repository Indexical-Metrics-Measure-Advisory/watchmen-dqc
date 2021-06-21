from apscheduler.schedulers.asyncio import AsyncIOScheduler

from dqc.config.config import settings
from dqc.job.schedule import global_rule_job
from dqc.job.schedule.expect_rule_job import run_expect_rule_set_for_topic_data


def init_jobs():
    if settings.JOB_FLAG:
        scheduler = AsyncIOScheduler()
        scheduler.add_job(global_rule_job.run, 'interval', seconds=10)
        scheduler.start()
