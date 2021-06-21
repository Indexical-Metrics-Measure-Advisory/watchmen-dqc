import json

import requests

from dqc.sdk.auth.auth_sdk import login
from dqc.sdk.utils.index import build_headers


def load_topic_list(site, names):
    headers = build_headers(login(site))
    response = requests.post(site["host"] + "topic/list/name", data=json.dumps(names),
                             headers=headers)

    return response.json()


def load_all_topic_list(site):
    headers = build_headers(login(site))
    response = requests.get(site["host"] + "topic/all",
                             headers=headers)
    return response.json()
