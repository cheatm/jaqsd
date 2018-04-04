from jaqsd.utils import api
from datetime import datetime, timedelta
import click
import pandas as pd
import os


def yesterday():
    t = datetime.today() - timedelta(1)
    return t.year*10000+t.month*100+t.day


def today():
    t = datetime.today()
    return t.year*10000+t.month*100+t.day


def day_shift(i=0):
    t = datetime.today() - timedelta(i)
    return t.year*10000+t.month*100+t.day




VIEW = click.argument("views", nargs=-1)
START = click.option("-s", "--start", default=None, type=click.INT)
START_STR = click.option("-s", "--start", default=None, type=click.STRING)
END_YESTERDAY = click.option("-e", "--end", default=day_shift(1), type=click.INT)
END_TODAY = click.option("-e", "--end", default=day_shift(0), type=click.INT)
END_TODAY_STR = click.option("-e", "--end", default=str(day_shift(1)), type=click.STRING)
END = click.option("-e", "--end", default=None, type=click.STRING)
SYMBOL = click.option("--symbol", default=None)
COVER = click.option("-c", "--cover", is_flag=True, default=False)
APPEND = click.option("-a", "--append", is_flag=True, default=False)
FORCE = click.option("-f", "--force", is_flag=True, default=False)


class TradeDayIndex(object):

    def __init__(self, root, name, columns):
        self.root = root
        self.name = os.path.join(root, name)
        self._columns = columns
        self._index = "trade_date"
        if os.path.isfile(self.name):
            self.table = pd.read_excel(self.name, index_col=self._index)
        else:
            self.table = pd.DataFrame(columns=columns)
            self.table.index.name = self._index

    def flush(self):
        self.table.to_excel(self.name)

    def create(self, start=None, end=None, append=False):
        dates = api.trade_day_index(start, end)
        if append:
            table = pd.DataFrame(self.table, dates, self._columns)
            table.fillna(0, inplace=True)
        else:
            table = pd.DataFrame(0, dates, self._columns)
        self.table = table
        self.flush()
        return table

    def search_range(self, view, start=None, end=None, cover=False):
        sliced = slice(*self.table.index.slice_locs(start, end, kind="loc"))
        series = self.table[view].iloc[sliced]
        if cover:
            return series.index
        else:
            return series[series == 0].index

    def fill(self, name, date, value):
        self.table.loc[date, name] = value


import logging


def logger(tag, *keys):
    formatter = "%s | %s" % (tag, " | ".join(["%s"]*(len(keys)+1)))

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