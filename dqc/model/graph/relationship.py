from pydantic import BaseModel


class Relationship(BaseModel):
    relationshipId: str = None
    fromId: str = None
    toId: str = None
    relationshipType: str = None
