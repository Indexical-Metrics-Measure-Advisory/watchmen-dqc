from datetime import datetime

from fastapi import APIRouter, Depends

from dqc.common import deps
from dqc.database.storage import catalog_storage
from dqc.model.catalog.catalog import Catalog, CatalogCriteria

router = APIRouter()


@router.post("/dqc/catalog/query", tags=["catalog"])
async def query_catalog(catalog_criteria: CatalogCriteria, current_user=Depends(deps.get_current_user)):
    return await catalog_storage.query_catalog_by_criteria(catalog_criteria,current_user)


@router.post("/dqc/catalog", tags=["catalog"])
async def create_catalog(catalog: Catalog, current_user=Depends(deps.get_current_user)):
    catalog.tenantId = current_user.tenantId
    catalog.createTime =datetime.now().replace(tzinfo=None).isoformat()
    return await catalog_storage.create_catalog_to_storage(catalog)


@router.post("/dqc/update/catalog", tags=["catalog"])
async def update_catalog(catalog_id: str, catalog: Catalog, current_user=Depends(deps.get_current_user)):
    return await catalog_storage.update_catalog_to_storage(catalog, catalog_id)


@router.post("dqc/catalog/delete", tags=["catalog"])
async def delete_catalog(catalog_id: str, catalog: Catalog, current_user=Depends(deps.get_current_user)):
    pass

