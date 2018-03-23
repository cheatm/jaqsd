from jaqsd import conf
from jaqsd.utils.api import get_api
from jaqsd.utils.structure import InstrumentInfo, Income, BalanceSheet, CashFlow, SecTradeCal
from datetime import datetime
from pymongo import MongoClient, InsertOne
import pandas as pd
import os
import logging
import click


UNIQUE = ["symbol", "ann_date", "report_type", "report_date"]

TABLES = {
    Income.view: Income,
    BalanceSheet.view: BalanceSheet,
    CashFlow.view: CashFlow
}


def get_data(api, query, **kwargs):
    data, msg = api.query(**query(**kwargs))
    if msg == "0,":
        return data
    else:
        raise Exception(msg)


def get_symbols(api):
    if "symbols" in globals():
        return globals()["symbols"]
    else:
        data, msg = api.query(**InstrumentInfo(inst_type="1", market="SZ,SH"))
        if msg == "0,":
            s = ",".join(data["symbol"])
            globals()["symbols"] = s
            return s


def trade_day_index(api, start, end):
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


class DailyIndex(object):

    def __init__(self, root, api):
        self.api = api
        self.root = root
        self.name = os.path.join(root, "daily.xlsx")
        if os.path.isfile(self.name):
            self.table = pd.read_excel(self.name, index_col="trade_date")

    def create(self, start=None, end=None):
        dates = trade_day_index(self.api, start, end)
        table = pd.DataFrame(0, dates, list(TABLES.keys()))
        self.table = table
        self.flush()
        return table

    def flush(self):
        self.table.to_excel(self.name)

    # def pull(self, view, symbols=None, start=None, end=None, cover=False):
    #     return pd.concat(list(self.iter_daily(view, symbols, start, end, cover)), ignore_index=True)

    def search_range(self, view, start=None, end=None, cover=False):
        sliced = slice(*self.table.index.slice_locs(start, end, kind="loc"))
        if cover:
            return self.table.index[sliced]
        else:
            series = self.table[view].iloc[sliced]
            return series[series == 0].index

    def iter_daily(self, view, symbol, start=None, end=None, cover=False):
        query = TABLES[view]
        for date in self.search_range(view, start, end, cover):
            try:
                data = get_data(self.api, query, symbol=symbol, start_date=date, end_date=date)
            except Exception as e:
                logging.error(" | ".join((view, date, e)))
            else:
                yield date, data

    def fill(self, view, date, value):
        self.table.loc[date, view] = value


class DailyDBWriter(object):

    def __init__(self, client):
        self.client = client

    def get_col(self, view):
        db_col = conf.get_col(view)
        db, col = db_col.split('.')
        return self.client[db][col]

    def write(self, view, data):
        if isinstance(data, pd.DataFrame):
            col = self.get_col(view)
            return col.bulk_write([InsertOne(row.to_dict()) for name, row in data.iterrows()]).inserted_count
        else:
            raise TypeError("data should be pd.DataFrame not: %s" % type(data))


def write(view, symbol=None, start=None, end=None, cover=False):
    writer = get_writer()
    di = get_index()

    if symbol is None:
        symbol = get_symbols(di.api)
    if end is None:
        end = get_today()

    for date, data in di.iter_daily(view, symbol, start, end, cover):
        try:
            if len(data):
                result = writer.write(view, data)
            else:
                result = -1
        except Exception as e:
            logging.error("%s | %s | %s", view, str(date), e)
        else:
            logging.warning("%s | %s | %s", view, str(date), result)
            di.fill(view, date, result)
    di.flush()


def get_writer():
    try:
        return globals()["ddw"]
    except KeyError:
        globals()["ddw"] = DailyDBWriter(MongoClient(conf.uri()))
        return globals()["ddw"]


def get_index():
    try:
        return globals()["di"]
        # return DailyIndex(conf.CONF_DIR, get_api())
    except KeyError:
        globals()["di"] = DailyIndex(conf.CONF_DIR, get_api())
        return globals()["di"]


def get_today():
    t = datetime.today()
    return t.year*10000+t.month*100+t.day


@click.command("write")
@click.argument("views", nargs=-1)
@click.option("--symbol", default=None)
@click.option("-s", "--start", default=None, type=click.INT)
@click.option("-e", "--end", default=None, type=click.INT)
@click.option("-c", "--cover", is_flag=True, default=False)
def writes(views, symbol=None, start=None, end=None, cover=False):
    if len(views) == 0:
        views = list(TABLES.keys())

    if start and end:
        if start > end:
            logging.error("start({}) > end({})".format(start, end))
            return

    for view in views:
        write(view, symbol, start, end, cover)

@click.command("command")
@click.option("-s", "--start", default=None, type=click.INT)
@click.option("-e", "--end", default=None, type=click.INT)
def create(start, end):
    di = get_index()
    di.create(start, end)


group = click.Group(
    "group",
    {"write": writes,
     "create": create}
)


if __name__ == '__main__':
    conf.init()
    group()