import requests
from model.model.common.user import User

from dqc.config.config import settings


def login(site):
    login_data = {"username": site["username"], "password": site["password"], "grant_type": "password"}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(site["host"] + "login/access-token", data=login_data,
                             headers=headers)
    auth_token = response.json()["access_token"]
    # print(auth_token)s
    return auth_token


# login()


def validate_token(token):
    url = settings.WATCHMEN_HOST + "login/validate_token"
    response = requests.get(url=url, params={"token": token})
    user = response.json()
    return User.parse_obj(user)
