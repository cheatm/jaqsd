from jaqsd import conf
from jaqsd.utils import api
from jaqsd.utils.structure import InstrumentInfo
from jaqsd.utils.mongodb import update, get_collection, read, insert
from jaqsd.utils.tool import logger
import pandas as pd
import logging


def get_new_list(date):
    data, msg = api.get_api().query(**InstrumentInfo(start_delistdate=date))
    return data.set_index("symbol")


def update_instrument_info(date):
    collection = get_collection("jz.instrumentInfo")
    try:
        data = get_new_list(date)
        print(data["inst_type"])
        if len(data.index) > 0:
            result = update(collection, data)
        else:
            result = 0
    except Exception as e:
        logging.error("update | instrument info | %s", e)
    else:
        logging.warning("update | instrument info | %s", result)


from datetime import datetime, timedelta


def yesterday():
    date = datetime.today() - timedelta(1)
    return date.year*10000+date.month*100+date.day


def catch_mi(symbol):
    code, ex = symbol.split(".", 1)
    for i in range(len(code)):
        if code[i].isnumeric():
            code = code[:i]
            break
    return "%s.%s" % (code, ex)


class InstTable(object):

    def __init__(self, collection):
        self.collection = collection

    def valid_symbol(self, regex=None, date=0, fields=None, **kwargs):
        filters = {"delist_date": {"$gte": str(date)}}
        for key, value in kwargs.items():
            if isinstance(value, list):
                filters[key] = {"$in": value}
            else:
                filters[key] = value
        if regex is not None:
            filters['symbol'] = {"$regex": regex}
        return read(self.collection, "symbol",
                    fields=fields,
                    filters=filters)

    def valid_future_symbols(self, date=0):
        result = self.valid_symbol(date=date,
                                   fields=["symbol"],
                                   inst_type=[101, 102, 103])
        result["title"] = list(map(catch_mi, result.index))
        return result.reset_index().set_index("title")["symbol"]

    def future_contract_date_range(self, regex=None, **kwargs):
        table = self.valid_symbol(regex, fields=["delist_date", "list_date", "symbol"], **kwargs)
        grouper = list(map(catch_mi, table.index))
        result = table.groupby(grouper).apply(lambda df: pd.Series({"list_date": df.list_date.min(),
                                                                    "delist_date": df.delist_date.max()}))
        return result

    def check(self):
        info = self.collection.index_information()
        if "symbol_1" not in info:
            self.collection.create_index("symbol", background=True)
            logging.warning("instrument | check | index 'symbol' not exists | create index 'symbol'")
        else:
            logging.warning("instrument | check | index 'symbol' already exists")
        return self.collection.find().count()

    @logger("Pull instrument info", 1)
    def pull(self, date):
        data = get_new_list(date)
        if self.collection.find().count() > 0:
            return update(self.collection, data)
        else:
            return insert(self.collection, data)


import click


@click.command("update")
@click.argument("date", nargs=1, default=yesterday(), type=click.INT)
def update(date):
    it = InstTable(get_collection("jz.instrumentInfo"))
    it.pull(date)


@click.command("check")
def check():
    it = InstTable(get_collection("jz.instrumentInfo"))
    count = it.check()
    logging.warning("instrument | check | symbol count = %s", count)


group = click.Group(
    "instrument",
    {"write": update,
     "check": check}
)
