from typing import Any

import requests
from pydantic import BaseModel

from dqc.sdk.auth.auth_sdk import login
from dqc.sdk.utils.index import build_headers


class InstanceRequest(BaseModel):
    code: str = None
    data: Any = None


def import_instance(instance, site):
    headers = build_headers(login(site))

    print(instance)

    response = requests.post(site["host"] + "topic/data", data=instance.json(),
                             headers=headers)

    print(response.status_code)
    if response.status_code == 200:
        return response.json()
    else:
        print(response.text)

    # return results
