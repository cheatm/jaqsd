from jaqsd import conf
from jaqsd.utils.mongodb import METHODS
from jaqsd.utils import api
from jaqsd.utils.tool import TradeDayIndex
import pandas as pd
from jaqsd.utils.structure import SecDailyIndicator


FIELDS = set(SecDailyIndicator.fields)
FIELDS.remove("trade_date")
FIELDS.remove("symbol")


class FieldIndex(TradeDayIndex):

    def __init__(self, root):
        super(FieldIndex, self).__init__(root, "dailyIndicator.xlsx", FIELDS)

    def iter_fields(self, symbol, start=None, end=None, cover=False):
        for start_date, end_date, columns in self.param_ranges(start, end, cover):
            data = api.sec_daily_indicator(symbol, *columns, start_date=start_date, end_date=end_date)
            data["symbol"] = data["symbol"].apply(fold)
            yield from iter_frame(data)

    def param_ranges(self, start=None, end=None, cover=False, max_count=5):
        table = self.table.loc[start:end]
        if cover:
            for i in range(0, len(table.index), max_count):
                idx = table.index[i:i+max_count]
                yield idx[0], idx[-1], table.columns
        else:
            frame = table == 0
            start = frame.index[0]
            end = start
            cache = frame.iloc[0]
            count = 0
            for key, series in frame.iterrows():
                if count < max_count:
                    if series.equals(cache):
                        end = key
                        count += 1
                    else:
                        yield start, end, cache[cache].index
                        cache = series
                        count = 0
                        start = key
                        end = key
                else:
                    yield start, end, cache[cache].index
                    cache = series
                    count = 0
                    start = key
                    end = key

            yield start, end, cache[cache].index


class FieldsWriter(object):

    def __init__(self, db):
        self.db = db

    def write(self, name, data, how='insert'):
        col = self.db[name]
        method = METHODS[how]
        return col.bulk_write(list(method(data)))

    def check(self, name, date):
        return self.db[name].find({"trade_date": date}).count()


def iter_frame(frame, symbol="symbol", index="trade_date"):
    if isinstance(frame, pd.DataFrame):
        yield from frame.set_index([index, symbol]).to_panel().iteritems()


def fold(s):
    return s[:6]


if __name__ == '__main__':
    from itertools import product

    conf.init("D:/jaqsd/conf")
    fi = FieldIndex(conf.CONF_DIR)
    fi.create(20170101, 20180101)
    writer = FieldsWriter(conf.get_client()["SecDailyIndicator"])
    for name, date in product(fi.table.columns, fi.table.index[:10]):
        print(name, date, writer.check(name, str(date)))