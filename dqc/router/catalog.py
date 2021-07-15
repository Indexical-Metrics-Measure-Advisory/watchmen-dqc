from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel
from storage.mongo.mongo_new_template import update_one_with_key
from storage.storage.storage_template import insert_one, find_one

from dqc.common.simpleflake import get_next_id
from dqc.model.catalog.data_source import DataSource
from dqc.service.catalog.index import fetch_from_watchmen_instance
from dqc.service.graph.index import GraphBuilder

router = APIRouter()


class WatchmenInstanceSite(BaseModel):
    instanceId: str = None
    url: str = None
    name: str = None
    username: str = None
    password: str = None
    protocol: str = None
    token: str = None


## add watchmen instance meta
@router.post("/dqc/instance/site", tags=["admin"], response_model=WatchmenInstanceSite)
async def add_watchmen_instance_site(watchmen_instance: WatchmenInstanceSite):
    watchmen_instance.instanceId = get_next_id()
    return insert_one(watchmen_instance, WatchmenInstanceSite, "watchmen_instance_sites")


## update watchmen instance meta
@router.post("/dqc/instance/site/update", tags=["admin"], response_model=WatchmenInstanceSite)
async def update_watchmen_instance_site(watchmen_instance: WatchmenInstanceSite):
    return update_one_with_key(watchmen_instance, WatchmenInstanceSite, "watchmen_instance_sites", "instanceId")


## fetch watchmen catalog
@router.get("/dqc/catalog/fetch/name", tags=["admin"], response_model=DataSource)
async def fetch_watchmen_catalog(watchmen_instance_name: str):
    site: WatchmenInstanceSite = find_one({"name": watchmen_instance_name}, WatchmenInstanceSite,
                                          "watchmen_instance_sites")
    # print(site)
    data_source = fetch_from_watchmen_instance(site)
    print(data_source)

    data = find_one({"name": watchmen_instance_name}, DataSource, "data_catalogs")
    print(data)
    if data is None:
        insert_one(data_source, DataSource, "data_catalogs")
    else:
        update_one_with_key(data_source, DataSource, "data_catalogs", "name")


## update watchmen catalog


## delete watchmen catalog
async def delete_watchmen_catalog():
    pass


## find node by type and name
@router.get("/dqc/catalog/graph/find", tags=["admin"], response_model=Any)
async def find_node_by_type_and_name(instance_name: str, node_type: str, node_name: str):
    data: DataSource = find_one({"name": instance_name}, DataSource, "data_catalogs")
    builder = GraphBuilder()
    builder.load_from_file(data.mlPath)
    data = builder.find(node_type=node_type, node_name=node_name)
    # print(data.attributes())
    return data.attributes()


## find node relations by condition
# TODO


## add rules on topic node
async def add_rules_on_topic(topic_name: str, rules):
    pass


## add rules on factor node
async def add_rules_on_factor(factor_name: str, rules):
    pass
