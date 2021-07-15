from datetime import datetime

from dqc.model.catalog.topic_knn_result import TopicKnnResult
from dqc.model.summary_model import SummaryModel


class TopicSummary(SummaryModel):
    topicId: str = None
    factorCount: int = None
    rowCount: int = None
    topicKnnResult: TopicKnnResult = None
    summaryGeneratorDate: datetime = None
