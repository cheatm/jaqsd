from jaqsd import conf
from jaqsd.utils import api
from jaqsd.utils.structure import Income, BalanceSheet, CashFlow
from jaqsd.utils.tool import TradeDayIndex
from datetime import datetime
from pymongo import MongoClient, InsertOne
import pandas as pd
import logging
import click


UNIQUE = ["symbol", "ann_date", "report_type", "report_date"]

QUERIES = {
    Income.view: Income,
    BalanceSheet.view: BalanceSheet,
    CashFlow.view: CashFlow
}


def get_data(_api, query, **kwargs):
    data, msg = _api.query(**query(**kwargs))
    if msg == "0,":
        return data
    else:
        raise Exception(msg)


class DailyIndex(TradeDayIndex):

    def __init__(self, root, _api):
        super(DailyIndex, self).__init__(root, "daily.xlsx", list(QUERIES.keys()))
        self.api = _api

    def iter_daily(self, view, symbol, start=None, end=None, cover=False):
        query = QUERIES[view]
        for date in self.search_range(view, start, end, cover):
            try:
                data = get_data(self.api, query, symbol=symbol, start_date=date, end_date=date)
            except Exception as e:
                logging.error(" | ".join((view, date, e)))
            else:
                yield date, data

    def reach(self, view, symbol, start=None, end=None):
        query = QUERIES[view]
        for date in self.search_range(view, start, end):
            reachable, result = reach_one(self.api, query, symbol, date)
            if reachable:
                yield date, result
            else:
                logging.warning("%s | %s | unreachable", view, date)
                self.fill(view, date, -1)
        self.flush()


def reach_one(_api, query, symbol, date, retry=3):
    for i in range(retry):
        data = get_data(_api, query, symbol=symbol, start_date=date, end_date=date)
        if len(data.index) != 0:
            return True, data

    return False, None


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
            return col.bulk_write([InsertOne(values.dropna().to_dict()) for name, values in data.iterrows()]).inserted_count
        else:
            raise TypeError("data should be pd.DataFrame not: %s" % type(data))

    def check(self, view, *dates):
        col = self.get_col(view)
        for date in dates:
            try:
                result = check_count_by_ann_date(col, str(date))
            except Exception as e:
                logging.error("check | %s | %s | %s", view, date, e)
            else:
                logging.warning("check | %s | %s | %s", view, date, result)
                yield date, result


def check_count_by_ann_date(col, date):
    return col.find({"ann_date": date}).count()


def write(view, symbol=None, start=None, end=None, cover=False):
    writer = get_writer()
    di = get_index()

    if symbol is None:
        symbol = api.all_stock_symbol()
    if end is None:
        end = get_today()

    for date, data in di.iter_daily(view, symbol, start, end, cover):
        try:
            if len(data.index):
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


def get_index(login=True):
    try:
        return globals()["di"]
    except KeyError:
        data_api = api.get_api() if login else None
        globals()["di"] = DailyIndex(conf.CONF_DIR, data_api)
        return globals()["di"]


def get_today():
    t = datetime.today()
    return t.year*10000+t.month*100+t.day


VIEW = click.argument("views", nargs=-1)
START = click.option("-s", "--start", default=None, type=click.INT)
END = click.option("-e", "--end", default=get_today(), type=click.INT)
SYMBOL = click.option("--symbol", default=None)
COVER = click.option("-c", "--cover", is_flag=True, default=False)

@click.command("write")
@VIEW
@SYMBOL
@START
@END
@COVER
def writes(views, symbol=None, start=None, end=None, cover=False):
    if len(views) == 0:
        views = list(QUERIES.keys())

    if start and end:
        if start > end:
            logging.error("start({}) > end({})".format(start, end))
            return

    for view in views:
        write(view, symbol, start, end, cover)


@click.command("create")
@START
@END
def create(start, end):
    di = get_index()
    di.create(start, end)


@click.command("check")
@VIEW
@START
@END
@COVER
def check(views, start, end, cover=False):
    if len(views) == 0:
        views = list(QUERIES.keys())

    writer = get_writer()
    di = get_index(False)
    for name in views:
        dates = di.search_range(name, start, end, cover)
        for date, count in writer.check(name, *dates):
            di.fill(name, date, count)
        di.flush()


@click.command("reach")
@VIEW
@START
@END
def reach(views, start, end):
    if len(views) == 0:
        views = list(QUERIES.keys())

    di = get_index()
    writer = get_writer()
    for name in views:
        reachable = dict(di.reach(name, api.all_stock_symbol(), start, end))
        for date, data in reachable.items():
            try:
                if len(data.index):
                    result = writer.write(name, data)
                else:
                    result = 0
            except Exception as e:
                logging.error("write | %s | %s | %s", name, str(date), e)
            else:
                logging.warning("write | %s | %s | %s", name, str(date), result)


group = click.Group(
    "group",
    {"write": writes,
     "create": create,
     "check": check,
     "reach": reach}
)


if __name__ == '__main__':
    conf.init()
    group()