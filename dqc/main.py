import logging

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from dqc.common.log import log
from dqc.config.config import settings
# from dqc.job.index import init_jobs
from dqc.router import common, analysis, catalog, execute_rule

log.init()

log = logging.getLogger("app." + __name__)

app = FastAPI(title=settings.PROJECT_NAME, version="0.1.0", description="watchmen data quality center")

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(analysis.router)
app.include_router(common.router)
app.include_router(catalog.router)
app.include_router(execute_rule.router)

"""
if settings.JOB_FLAG:
    init_jobs()
"""
