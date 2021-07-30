import json

import requests

from dqc.config.config import settings
from dqc.sdk.utils.index import build_headers


def load_topic_list(names):
    headers = build_headers()
    response = requests.post(settings.WATCHMEN_HOST + "topic/list/name", data=json.dumps(names),
                             headers=headers)

    return response.json()


def load_all_topic_list():
    headers = build_headers()
    # print(headers)
    response = requests.get(settings.WATCHMEN_HOST + "topic/all",
                            headers=headers)

    # print(response.text)
    return response.json()


def get_topic_by_id(topic_id):
    headers = build_headers()
    response = requests.get(settings.WATCHMEN_HOST + "topic", params={"topic_id": topic_id},
                            headers=headers)
    return response.json()
