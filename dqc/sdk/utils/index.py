from dqc.config.config import settings


def build_headers():
    headers = {"Content-Type": "application/json", "Authorization": "pat " + settings.WATCHMEN_PAT}
    return headers
