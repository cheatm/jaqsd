from jaqsd.utils import api
from datetime import datetime, timedelta
import click
import pandas as pd
import os


def yesterday():
    t = datetime.today() - timedelta(1)
    return t.year*10000+t.month*100+t.day


VIEW = click.argument("views", nargs=-1)
START = click.option("-s", "--start", default=None, type=click.INT)
END_YESTERDAY = click.option("-e", "--end", default=yesterday(), type=click.INT)
END = click.option("-e", "--end", default=None, type=click.INT)
SYMBOL = click.option("--symbol", default=None)
COVER = click.option("-c", "--cover", is_flag=True, default=False)


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

    def create(self, start=None, end=None):
        dates = api.trade_day_index(start, end)
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

