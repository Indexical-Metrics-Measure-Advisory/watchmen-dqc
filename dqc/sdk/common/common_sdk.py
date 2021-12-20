from typing import Any

import requests
from model.model.common.user import User
from pydantic import BaseModel
from watchmen_boot.storage.model.data_source import DataSource

from dqc.config.config import settings
from dqc.sdk.utils.index import build_headers


class InstanceRequest(BaseModel):
    code: str = None
    data: Any = None
    tenantId: str = None


def import_instance(instance, current_user: User):
    headers = build_headers(current_user)
    response = requests.post(settings.WATCHMEN_HOST + "pipeline/data/async/tenant", data=instance.json(),
                             headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(response.text)


def get_datasource_by_id(datasource_id):
    headers = build_headers()
    response = requests.get(settings.WATCHMEN_HOST + "datasource/id", params={"datasource_id": datasource_id},
                            headers=headers)
    return DataSource.parse_obj(response.json())
