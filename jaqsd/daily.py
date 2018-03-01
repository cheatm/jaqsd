from jaqsd import conf, indexes
from jaqs.data import DataApi
from datetime import datetime
from pymongo import UpdateOne, MongoClient
import logging


columns = ["open", "high", "low", "close", "volume", "turnover"]
fields = ",".join(columns)


class IdxWriter(object):

    def __init__(self, db, api):
        self.db = db
        self.api = api

    def write(self, codes, start=0, end=99999999):
        for code in codes:
            try:
                result = self._write(code, start, end)
            except Exception as e:
                logging.error("%s | %s-%s | %s", code, start, end, e)
            else:
                logging.warning("%s | %s-%s | %s",  code, start, end, result)

    def check_index(self, code):
        name = expand(code)
        info = self.db[name].index_information()
        if "datetime_1" in info:
            logging.warning("%s | index_ok", code)
        else:
            logging.warning("%s | no_index", code)
            result = self.db[name].create_index("datetime", unique=True, background=True)
            logging.warning("%s | create_index | %s", code, result)

    def _write(self, code, start=0, end=99999999):
        data = read(self.api, code, start, end)
        name = expand(code)
        return update(self.db[name], data)


def update(collection, data):
    return collection.bulk_write(
        [UpdateOne({"datetime": time}, {"$set": series.to_dict()}, True) for time, series in data.iterrows()]
    ).upserted_count


def num2date(num):
    year = int(num/10000)
    md = num % 10000
    month = int(md/100)
    day = int(md % 100)
    return datetime(year, month, day, 15)


def read(api, code, start=0, end=99999999):
    data, msg = api.daily(code, start, end, fields=fields)
    if msg == "0,":
        data.index = data['trade_date'].apply(num2date)
        out = data[columns]
        out['datetime'] = out.index
        return out
    else:
        raise Exception(msg)


def expand(code):
    if code.startswith("0"):
        return code[:6] + ".XSHG"
    elif code.startswith("399"):
        return code[:6] + ".XSHE"


def check(codes):
    writer = IdxWriter(get_db(), None)
    for code in codes:
        try:
            writer.check_index(code)
        except Exception as e:
            logging.error("%s | check_index | %s", code, e)


def write(codes, start, end):
    writer = IdxWriter(get_db(), get_api())
    writer.write(codes, start, end)


def get_db():
    return MongoClient(conf.uri())[conf.daily()]


def get_api():
    api = DataApi()
    api.login(conf.username(), conf.password())
    return api


import click
from datetime import datetime

today = int(datetime.now().strftime("%Y%m%d"))


def DEFAULT(func):
    def wrapped(**kwargs):
        if len(kwargs.get("codes", [])) == 0:
            kwargs['codes'] = indexes.codes
        return func(**kwargs)

    wrapped.__name__ = func.__name__
    return wrapped


CODES = click.Argument(["codes"], nargs=-1)
START = click.Option(["-s", "--start"], default=today, type=click.INT)
END = click.Option(["-e", "--end"], default=99999999, type=click.INT)


cmd_write = click.Command(
    "write", callback=DEFAULT(write),
    params=[CODES, START, END]
)

cmd_check = click.Command(
    "check", callback=DEFAULT(check),
    params=[CODES]
)


group = click.Group(
    "daily",
    {"write": cmd_write,
     "check": cmd_check}
)


if __name__ == '__main__':
    conf.init()
    indexes.init()
    group()