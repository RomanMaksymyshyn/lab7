import base64
import json
import random
import socket
from contextlib import closing

import requests


def register_self(service_name, port, address="http://127.0.0.1"):
    services = get_services(service_name)
    service_id = service_name + str(len(services))
    payload = {
        "id": service_id,
        "name": service_name,
        "port": port,
        "address": address
    }
    print("register " + service_id + " in consul")
    requests.put("http://127.0.0.1:8500/v1/agent/service/register", data=json.dumps(payload))
    return len(services)


def get_service(service_name):
    services = get_services(service_name)
    if len(services) > 0:
        return random.choice(services)


def get_services(service_name):
    resp = requests.get("http://127.0.0.1:8500/v1/catalog/service/" + service_name)
    if resp.status_code == 200:
        existing_nodes = resp.json()
        return existing_nodes
    else:
        return []


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def get_val(key):
    resp = requests.get("http://localhost:8500/v1/kv/" + key)
    return base64.b64decode(resp.json()[0]["Value"]).decode('utf-8')

