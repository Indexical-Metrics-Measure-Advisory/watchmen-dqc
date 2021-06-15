from pydantic import BaseModel


class GraphNode(BaseModel):
    nodeId: str = None
    name: str = None
    nodeType: str = None
    nodeRefId: str = None
