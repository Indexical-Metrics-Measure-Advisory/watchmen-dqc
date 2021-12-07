from model.model.common.user import User

from dqc.config.config import settings
from dqc.sdk.auth.auth_sdk import login


def build_headers(current_user:User=None):
    if current_user:
        access_token = login(current_user)
        headers = {"Content-Type": "application/json", "Authorization": "Bearer " + access_token}
        return headers
    else:
        headers = {"Content-Type": "application/json", "Authorization": "pat " + settings.WATCHMEN_PAT}
        return headers
