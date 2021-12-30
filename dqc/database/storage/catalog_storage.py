from watchmen_boot.guid.snowflake import get_surrogate_key

from dqc.database.find_storage_template import find_storage_template
from dqc.model.catalog.catalog import Catalog, CatalogCriteria

CATALOGS = "catalogs"

storage_template = find_storage_template()


async def create_catalog_to_storage(catalog: Catalog):
    catalog.catalogId = get_surrogate_key()
    return storage_template.insert_one(catalog, Catalog, CATALOGS)


async def update_catalog_to_storage(catalog: Catalog, catalog_id: str):
    return storage_template.update_one_first({"catalogid": catalog_id}, catalog, Catalog, CATALOGS)


async def find_catalogs_by_name(query_name: str, current_user):
    return storage_template.find_({"and": [{"name": {"like": query_name}}, {"tenantId": current_user.tenantId}]},
                                  Catalog, CATALOGS)


async def load_catalog_by_id(catalog_id: str, current_user):
    return storage_template.find_one({"and": [{"catalogid": catalog_id}, {"tenantId": current_user.tenantId}]},
                                     Catalog, CATALOGS
                                     )

async def query_catalog_by_criteria(criteria:CatalogCriteria,current_user):

    if criteria.name is None and criteria.topicId is None and criteria.techOwnerId is None and criteria.bizOwnerId is None:
        return storage_template.find_({"tenantId":current_user.tenantId}, Catalog, CATALOGS)

    return storage_template.find_({"and":[{"name":{"like":criteria.name}}]}, Catalog, CATALOGS)

async def load_catalogs(current_user):
    return storage_template.find_({"tenantId": current_user.tenantId}, Catalog, CATALOGS)