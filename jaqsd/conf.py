import logging
import os
import yaml

_config = {}

CONF_DIR = os.environ.get("JAQSD_CONF_DIR", "/conf")
FILE = "config.yml"


def load(path, default=dict):
    try:
        return yaml.load(open(os.path.join(CONF_DIR, path)).read())
    except:
        return default()


def init(root=None):
    if root:
        globals()["CONF_DIR"] = root
    globals()["_config"] = load(FILE)


def uri():
    return _config.get("mongodb_uri", "localhost:27017")


def daily():
    return _config.get("daily", "Stock_D")


def username():
    return _config.get("username", "username")


def password():
    return _config.get("password", "password")


def col_map():
    return _config.get("col_map")


def db_map():
    return _config.get("db_map")


def get_col(view):
    return col_map().get(view, view)


def get_db(view):
    return db_map()[view]


def get_client():
    from pymongo import MongoClient
    return MongoClient(uri())


logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
    )