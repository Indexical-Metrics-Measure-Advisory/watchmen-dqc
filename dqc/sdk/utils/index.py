from dqc.config.config import settings
from dqc.model.token_user import TokenUser


def build_headers(current_user: TokenUser = None):
    if current_user:
        # access_token = login(current_user)
        headers = {"Content-Type": "application/json", "Authorization": "Bearer " + current_user.token}
        return headers
    else:
        headers = {"Content-Type": "application/json", "Authorization": "pat " + settings.WATCHMEN_PAT}
        return headers
