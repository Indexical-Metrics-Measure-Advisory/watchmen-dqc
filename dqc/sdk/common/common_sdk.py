from typing import Any

import requests
from pydantic import BaseModel

from dqc.config.config import settings
from dqc.sdk.auth.auth_sdk import login
from dqc.sdk.utils.index import build_headers


class InstanceRequest(BaseModel):
    code: str = None
    data: Any = None


def import_instance(instance):
    headers = build_headers()
    response = requests.post(settings.WATCHMEN_HOST + "topic/data", data=instance.json(),
                             headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(response.text)


