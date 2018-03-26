import jaqsd.conf as conf
from jaqs.data import DataApi
from jaqsd.utils.structure import *
from jaqsd.utils.singleton import single
from functools import wraps
import logging


@single("_api")
def get_api(username=None, password=None):
    u = username if username else conf.username()
    p = password if password else conf.password()
    api = DataApi()
    api.login(u, p)
    logging.warning("DataApi login | username=%s | password=%s", u, p)
    return api


def reply_check(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        data, msg = func(*args, **kwargs)
        if msg == "0,":
            return data
        else:
            raise Exception(msg)
    return wrapped


@reply_check
def sec_daily_indicator(symbol, *fields, **kwargs):
    return get_api().query(**SecDailyIndicator(*fields, symbol=symbol, **kwargs))


def all_stock_symbol():
    try:
        return globals()["symbols"]
    except KeyError:
        data, msg = get_api().query(**InstrumentInfo(inst_type="1", market="SZ,SH"))
        if msg == "0,":
            s = ",".join(data["symbol"])
            globals()["symbols"] = s
            return s


def trade_day_index(start, end, api=None):
    if api is None:
        api = get_api()
    params = {}
    if start:
        params["start_date"] = start
    if end:
        params["end_date"] = end
    data, msg = api.query(**SecTradeCal("istradeday", "trade_date", **params))
    if msg == "0,":
        return data.set_index("trade_date").index
    else:
        raise Exception(msg)


def close():
    get_api().close()
    single.delete("_api")