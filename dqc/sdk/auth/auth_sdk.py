import requests
from model.model.common.user import User

from dqc.config.config import settings
from dqc.model.token_user import TokenUser


def login(user: User):
    login_data = {"username": user.name, "password": user.password, "grant_type": "password"}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(settings.WATCHMEN_HOST + "login/access-token", data=login_data,
                             headers=headers)
    auth_token = response.json()["access_token"]
    return auth_token


def validate_token(token):
    url = settings.WATCHMEN_HOST + "login/validate_token"
    response = requests.get(url=url, params={"token": token})
    user = TokenUser.parse_obj(response.json())
    user.token = token
    return user
