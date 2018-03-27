from jaqsd import conf
from jaqsd.utils.mongodb import METHODS
from jaqsd.utils import api
from jaqsd.utils.tool import TradeDayIndex, START, END_TODAY, COVER, END
import pandas as pd
import logging
from jaqsd.utils.structure import SecDailyIndicator
import click


FIELDS = set(SecDailyIndicator.fields)
FIELDS.remove("trade_date")
FIELDS.remove("symbol")


def logger(*keys):
    formatter = " | ".join(["%s"]*(len(keys)+1))

    def select(*args, **kwargs):
        for key in keys:
            if isinstance(key, int):
                yield args[key]
            else:
                yield kwargs[key]

    def wrapper(func):
        def wrapped(*args, **kwargs):
            show = list(select(*args, **kwargs))
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                show.append(e)
                logging.error(formatter, *show)
            else:
                show.append(result)
                logging.warning(formatter, *show)
                return result
        return wrapped
    return wrapper


class FieldIndex(TradeDayIndex):

    def __init__(self, root):
        super(FieldIndex, self).__init__(root, "dailyIndicator.xlsx", FIELDS)

    @staticmethod
    def _get_data(symbol, start, end, field):
        try:
            data = api.sec_daily_indicator(symbol, *field, start_date=start, end_date=end)
            data["symbol"] = data["symbol"].apply(fold)
            if "limit_status" in data.columns:
                data["limit_status"] = data["limit_status"].apply(float)
        except Exception as e:
            logging.error("require | %s | %s | %s", start, end, e)
        else:
            return data

    def iter_data(self, symbol, fields=None, start=None, end=None, cover=False, how="date"):
        if fields is None:
            fields = []
        if how == 'date':
            for date, field in self.iter_dates(start, end, *fields, cover=cover):
                data = self._get_data(symbol, date, date, field)
                if isinstance(data, pd.DataFrame) and len(data.index):
                    yield from iter_frame(data)
        elif how == "field":
            for field, dates in self.iter_fields(start, end, *fields, cover=cover):
                data = self._get_data(symbol, dates[0], dates[-1], [field])
                if isinstance(data, pd.DataFrame) and len(data.index):
                    _dates = list(map(str, dates))
                    for name, frame in iter_frame(data):
                        yield name, frame.loc[_dates]

    def loc(self, start, end, *fields):
        if len(fields):
            return self.table.loc[start:end, list(fields)]
        else:
            return self.table.loc[start:end]

    def iter_dates(self, start, end, *fields, cover=False):
        table = self.loc(start, end, *fields)
        for date, row in table.iterrows():
            if cover:
                yield date, row.index
            else:
                yield date, row[row==0].index

    def iter_fields(self, start, end, *fields, cover=False):
        table = self.loc(start, end, *fields)
        for name, item in table.iteritems():
            if cover:
                yield name, list(item.index)
            else:
                for dates in iter_ranges(item, 0):
                    yield name, dates

    def iter_ranges(self, start, end, *fields, cover=False):
        table = self.loc(start, end, *fields)

        for name, series in table.iteritems():
            if cover:
                for date in series.index:
                    yield name, date
            else:
                for date in series[series==0].index:
                    yield name, date


def iter_ranges(series, value=0, max_length=10):
    cache = []
    for key, item in series.iteritems():
        if len(cache) >= max_length:
            yield cache
            cache = []
        if item == value:
            cache.append(key)
        else:
            if len(cache):
                yield cache
                cache = []
    if len(cache):
        yield cache


class FieldsWriter(object):

    def __init__(self, db):
        self.db = db

    @logger(3, 1)
    def write(self, name, data, how='insert'):
        col = self.db[name]
        method = METHODS[how]
        result = col.bulk_write(list(method(data)))
        if how == 'insert':
            return result.inserted_count
        elif how == "update":
            return result.upserted_count
        else:
            return result

    @logger(1, 2)
    def check(self, name, date):
        return self.db[name].find({"trade_date": date}).count()


def iter_frame(frame, symbol="symbol", index="trade_date"):
    if isinstance(frame, pd.DataFrame):
        yield from frame.set_index([index, symbol]).to_panel().iteritems()


def fold(s):
    return s[:6]


@click.command("create")
@START
@END
def create(start, end):
    fi = FieldIndex(conf.CONF_DIR)
    fi.create(start, end)
    fi.flush()


@click.command("write")
@click.argument("fields", nargs=-1)
@START
@END_TODAY
@COVER
@click.option('-a', "--axis", default="date")
@click.option('-h', "--how", default="insert")
def write(fields, start=None, end=None, cover=False, axis='date', how="insert"):
    fi = FieldIndex(conf.CONF_DIR)
    writer = FieldsWriter(conf.get_client()[conf.get_db("lb.secDailyIndicator")])
    for name, item in fi.iter_data(api.all_stock_symbol(), fields, start=start, end=end, cover=cover, how=axis):
        writer.write(name, item, how)


@click.command("check")
@click.argument("fields", nargs=-1)
@START
@END_TODAY
@COVER
def check(fields, start=None, end=None, cover=False):
    fi = FieldIndex(conf.CONF_DIR)
    writer = FieldsWriter(conf.get_client()[conf.get_db("lb.secDailyIndicator")])
    for name, date in fi.iter_ranges(start, end, *fields, cover=cover):
        result = writer.check(name, str(date))
        if result == 1:
            fi.fill(name, date, 1)
    fi.flush()


group = click.Group(
    "group",
    {"check": check,
     "write": write,
     "create": create}
)


if __name__ == '__main__':
    conf.init()
    group()