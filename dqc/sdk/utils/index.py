from dqc.model.token_user import TokenUser

"""
def build_headers(current_user: TokenUser = None):
    if current_user:
        # access_token = login(current_user)
        headers = {"Content-Type": "application/json", "Authorization": "Bearer " + current_user.token}
        return headers
    else:
        headers = {"Content-Type": "application/json", "Authorization": "pat " + settings.WATCHMEN_PAT}
        return headers
"""


def build_headers(current_user: TokenUser):
    headers = {"Content-Type": "application/json", "Authorization": current_user.token}
    return headers
