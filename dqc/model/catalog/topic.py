from datetime import datetime

from pydantic import BaseModel

from dqc.model.analysis.topic_summary import TopicSummary


class Topic(BaseModel):
    name: str = None
    topicType: str = None
    topicKind: str = None
    topicSummary: TopicSummary = None
    # factor_dict: Dict[str, Factor] = {}
    owner: str = None
    lastUpdated: datetime = None
