import requests


# from dqc.router.catalog import WatchmenInstanceSite


def __build_headers(token):
    return {"Content-Type": "application/json", "Authorization": "Bearer " + token}


def __login(site):
    # login_data = {"username": "imma-admin", "password": "abc1234", "grant_type": "password"}
    # headers = {"Content-Type": "application/x-www-form-urlencoded"}
    # response = requests.post("http://localhost:8000/login/access-token", data=login_data,
    #                          headers=headers)
    # auth_token = response.json()["access_token"]
    # print(auth_token)
    # TODO login
    return site.token


def __build_basic_url(site):
    return site.protocol + site.url


def fetch_all_topics(site):
    basic_url = __build_basic_url(site)
    # print(basic_url+"/topic/all")
    headers = __build_headers(__login(site))
    response = requests.get(basic_url + "/topic/all",
                            headers=headers)
    # print(response.status_code)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def fetch_all_pipelines(site):
    basic_url = __build_basic_url(site)
    headers = __build_headers(__login(site))
    response = requests.get(basic_url + "/pipeline/all",
                            headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return None
