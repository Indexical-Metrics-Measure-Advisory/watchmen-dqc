import requests


def login(site):
    login_data = {"username": site["username"], "password": site["password"], "grant_type": "password"}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(site["host"] + "login/access-token", data=login_data,
                             headers=headers)
    auth_token = response.json()["access_token"]
    # print(auth_token)s
    return auth_token

# login()
