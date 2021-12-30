from typing import List

from model.model.common.watchmen_model import WatchmenModel
from pydantic import BaseModel


class Catalog(WatchmenModel):
    catalogId: str = None
    name: str = None
    topicIds: List[str] = []
    techOwnerId: str = None
    bizOwnerId: str = None
    tags: List[str] = []
    description: str = None
    tenantId: str = None


class CatalogCriteria(BaseModel):
    name: str = None
    topicId: str = None
    techOwnerId: str = None
    bizOwnerId: str = None
