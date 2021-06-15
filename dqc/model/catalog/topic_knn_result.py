from pydantic import BaseModel


class TopicKnnResult(BaseModel):
    clusterNum :int = None

