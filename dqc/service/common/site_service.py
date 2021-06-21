import json
from os import path


def load_site_json():
    data = {}
    if path.exists("site/site.json"):
        with open('site/site.json', 'r') as outfile:
            data = json.load(outfile)
            return data
    else:
        return data


def save_to_json(data):
    with open('site/site.json', 'w') as outfile:
        json.dump(data, outfile)
