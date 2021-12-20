import json

import requests
from model.model.common.user import User
from model.model.topic.topic import Topic

from dqc.config.config import settings
from dqc.model.token_user import TokenUser
from dqc.sdk.utils.index import build_headers


def load_topic_list(names, current_user: User):
    headers = build_headers(current_user)
    response = requests.post(settings.WATCHMEN_HOST + "topic/list/name", data=json.dumps(names),
                             headers=headers)

    return response.json()


def load_topic_by_name_and_tenant(name, tenant_id):
    headers = build_headers()
    response = requests.get(settings.WATCHMEN_HOST + "topic/name/tenant", params={"name": name, "tenant_id": tenant_id},
                            headers=headers)
    return Topic.parse_obj(response.json())


def load_all_topic_list():
    headers = build_headers()
    response = requests.get(settings.WATCHMEN_HOST + "topic/all/tenant",
                            headers=headers)

    results = response.json()
    return results


def get_topic_by_id(topic_id, current_user: TokenUser):
    headers = build_headers(current_user)
    response = requests.get(settings.WATCHMEN_HOST + "topic", params={"topic_id": topic_id},
                            headers=headers)
    return Topic.parse_obj(response.json())
