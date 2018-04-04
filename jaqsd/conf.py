import logging
import os
import yaml

_config = {'mongodb_uri': None,
           'username': '',
           'password': '',
           'col_map': {'lb.income': 'lb.income',
                       'lb.balanceSheet': 'lb.balanceSheet',
                       'lb.cashFlow': 'lb.cashFlow',
                       'lb.secAdjFactor': 'lb.secAdjFactor',
                       'lb.profitExpress': 'lb.profitExpress',
                       'lb.secDividend': 'lb.secDividend',
                       'lb.finIndicator': 'lb.finIndicator'},
           'db_map': {'lb.secDailyIndicator': 'SecDailyIndicator'},
           'tables': {"lb_daily": {"file": "daily.xlsx", "col": "log.lb_daily"},
                      "lb_dailyIndicator": {"file": "dailyIndicator.xlsx", "col": "log.dailyIndicator"}}}


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
    update(_config, load(FILE))
    # globals()["_config"] = load(FILE)


def update(origin, inputs):
    for key, value in inputs.items():
        if isinstance(value, dict):
            o = origin.setdefault(key, {})
            if isinstance(o, dict):
                update(o, value)
            else:
                raise TypeError("Default type of %s is not dict" % key)
        else:
            origin[key] = value


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


def get_tables():
    return _config.get("tables")


def get_file_path(name):
    return os.path.join(CONF_DIR, name)


logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
    )
