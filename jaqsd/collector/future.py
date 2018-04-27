from jaqsd.utils import api
from jaqsd import conf, variables
from jaqsd.utils.mongodb import get_collection, read, insert, update
from jaqsd.utils.tool import logger, WorkFlow
from jaqsd.collector.ins_info import InstTable
from datetime import datetime, timedelta
import pandas as pd
import logging

DATE = "_d"
LENGTH = "_l"

MI = ['CF.CZC', 'FG.CZC', 'JR.CZC', 'LR.CZC', 'MA.CZC', 'OI.CZC', 'PM.CZC', 'RI.CZC', 'RM.CZC', 'RS.CZC', 'SF.CZC',
      'SM.CZC', 'SR.CZC', 'TA.CZC', 'ZC.CZC', 'WH.CZC', 'ME.CZC', 'ER.CZC', 'RO.CZC', 'TC.CZC', 'WS.CZC', 'WT.CZC',
      'CY.CZC', 'AP.CZC', 'c.DCE', 'cs.DCE', 'a.DCE', 'b.DCE', 'm.DCE', 'y.DCE', 'p.DCE', 'bb.DCE', 'fb.DCE', 'i.DCE',
      'j.DCE', 'jd.DCE', 'jm.DCE', 'l.DCE', 'pp.DCE', 'v.DCE', 'ni.SHF', 'cu.SHF', 'al.SHF', 'zn.SHF', 'pb.SHF',
      'sn.SHF', 'au.SHF', 'ag.SHF', 'bu.SHF', 'fu.SHF', 'hc.SHF', 'rb.SHF', 'ru.SHF', 'wr.SHF', 'sc.INE']

F_MI = ["IC.CFE", "IF.CFE", "IH.CFE", "TF.CFE", "T.CFE"]


def date2num(date):
    return date.year*10000+date.month*100+date.day


def get_db():
    return conf.get_db("future")["1M"]


def get_future_1m_db():
    client = conf.get_client()
    return client[get_db()]


def insert_chunk(collection, data):
    doc = make_doc(data)
    collection.insert_one(doc)
    return 1


def update_chunk(collection, data):
    doc = make_doc(data)
    collection.update_one({DATE: doc[DATE]}, {"$set": doc}, upsert=True)
    return 1


methods = {"insert": insert_chunk,
           "update": update_chunk}


def make_doc(data):
    doc = data.reset_index().to_dict("list")
    date = data.index[-1]
    doc[DATE] = datetime(date.year, date.month, date.day)
    doc[LENGTH] = len(data.index)
    return doc


def create_commercial_index(start, end, valid_dates):
    table = pd.DataFrame(index=api.get_trade_days(start, end), columns=valid_dates.index)
    table.index.name = "date"
    table.columns.name = "symbol"
    for symbol, row in valid_dates.iterrows():
        end = row.delist_date
        if end > 20170101:
            end = 99999999
        table.loc[row.list_date:end, symbol] = 0
    return table


def line2point(string):
    return string.replace("_", ".")


def point2line(string):
    return string.replace(".", "_")


def int2date(num):
    day = num % 100
    month = int(num/100 % 100)
    year = int(num/10000)
    return datetime(year, month, day)


class FutureIndex(object):

    def __init__(self, collection):
        self.collection = collection

    def _read(self, names=None, start=datetime(1991, 1, 1), end=datetime(2029, 12, 31)):
        if names is not None:
            names = list(map(point2line, names))
        return read(self.collection, "date", start, end, names).rename_axis(line2point, 1)

    def read_index(self, names=None, start=datetime(1991, 1, 1), end=datetime(2029, 12, 31)):
        return self._read(names, start, end)

    def iter_index(self, names=None, start=datetime(1991, 1, 1), end=datetime(2029, 12, 31), cover=False):
        table = self._read(names, start, end)
        for name, item in table.iteritems():
            if cover:
                dates = item.index
            else:
                dates = item[item==0].index
            for date in dates:
                yield name, date

    def ensure_index(self):
        info = self.collection.index_information()
        if "date_1" not in info:
            self.collection.create_index('date', background=True, unique=True)

    def flush(self, table, how="insert"):
        table = table.rename_axis(point2line, 1)
        if how == "insert":
            return insert(self.collection, table)
        elif how == "append":
            return update(self.collection, table, how="$setOnInsert")
        else:
            return update(self.collection, table)


def join_dt(s):
    year = int(s.trade_date/10000)
    month = int(s.trade_date/100)%100
    day = s.trade_date%100
    hour = int(s.time/10000)
    minute = int(s.time/100)%100
    return datetime(year, month, day, hour, minute)


class CommercialWriter(object):

    def __init__(self, db, trade_days=None):
        self.db = db
        self.trade_days = pd.Index(api.get_trade_days()) if trade_days is None else trade_days

    def bar(self, symbol, date):
        if not isinstance(date, int):
            date = date2num(date)
        data, msg = api.get_api().bar(symbol, trade_date=date)
        if msg != "0,":
            raise ValueError(msg)
        if len(data.index) == 0:
            raise ValueError("data is empty")

        idx = self.trade_days.searchsorted(date)
        if idx > 0:
            data["trade_date"][data.time>=200000] = self.trade_days[idx-1]
        data["datetime"] = data[["trade_date", "time"]].agg(join_dt, 1)
        data = data[["open", "high", "low", "close", "volume", "turnover", "oi", "datetime"]].set_index("datetime")
        return data

    def writes(self, table, how="insert"):
        for symbol, date in iter_table(table):
            self.write(symbol, date, how)

    @logger("write min1", 1, 2, 3)
    def write(self, symbol, date, how="insert"):
        data = self.bar(symbol, date)
        method = methods[how]
        return method(self.db[symbol], data)

    @logger("check min1", 1, 2, default=lambda: 0)
    def check_chunk(self, symbol, date):
        return self.db[symbol].find({"_d": date}).count()


def catch_mi(symbol):
    code, ex = symbol.split(".", 1)
    for i in range(len(code)):
        if code[i].isnumeric():
            code = code[:i]
            break
    return "%s.%s" % (code, ex)


@logger("Find MI", 0, 1, 2)
def find_daily_mi(symbols, start=0, end=99999999):
    try:
        data, msg = api.get_api().daily(symbols, start, end, fields="symbol,oi,trade_date")
        if msg != "0,":
            raise ValueError(msg)
        grouper = data.symbol.apply(catch_mi)
        result = data.groupby(grouper).apply(lambda df: df.pivot("trade_date", "symbol", "oi").idxmax(1)).T
        if isinstance(result.index, pd.MultiIndex):
            result = result.unstack().T
    except Exception as e:
        logging.error("find main contracts | %s | %s | %s | %s", symbols, start, end, e)
    else:
        logging.warning("find main contracts | %s | %s | %s | %s", symbols, start, end, result.shape)
        return result


def iter_table(table, cover=False):
    for name, item in table.iteritems():
        if cover:
            dates = item.index
        else:
            dates = item[item==0].index
        for date in dates:
            yield name, date


class FutureMIFinder(object):

    def __init__(self, collection):
        self.collection = collection

    @logger("Write MI contracts")
    def write(self, data, how="update"):
        data = data.rename_axis(point2line, 1)
        if how == "append":
            return update(self.collection, data, how="$setOnInsert")
        else:
            return update(self.collection, data)

    def read(self, names, start, end):
        if names is not None:
            names = list(map(point2line, names))
        return read(self.collection, "trade_date", start, end, names).rename_axis(line2point, 1)


def rename(names):
    if names is not None and (len(names) == 0):
        return None
    else:
        return names


def check(names=None, start=19910101, end=20291231, cover=False, how="update"):
    names = rename(names)
    fi = FutureIndex(get_collection(variables.FUTURE_MI))
    cw = CommercialWriter(get_future_1m_db())
    table = fi.read_index(names, int2date(start), int2date(end))
    for symbol, date in iter_table(table):
        result = cw.check_chunk(symbol, date)
        table.loc[date, symbol] = result
    fi.flush(table, how)
    return table


def write_mi(names=None, start=19910101, end=20291231, how="insert"):
    if names is None:
        names = MI
    fi = FutureIndex(get_collection(variables.FUTURE_MI))
    cw = CommercialWriter(get_future_1m_db())
    table = fi._read(names, int2date(start), int2date(end))
    for symbol, date in iter_table(table):
        cw.write(symbol, date, how)


def write_none_mi(names=None, start=19910101, end=20291231, how="insert"):
    if names is None:
        names = F_MI
    fi = FutureIndex(get_collection(variables.FUTURE_MI))
    cw = CommercialWriter(get_future_1m_db())
    finder = FutureMIFinder(get_collection(variables.CONTRACTS))
    table = fi._read(names, int2date(start), int2date(end))
    contracts = finder.read(names, start, end).rename_axis(int2date).reindex(table.index).ffill()
    method = methods[how]
    for symbol, date in iter_table(table):
        try:
            cont = contracts.loc[date, symbol]
            data = cw.bar(cont, date)
            result = method(cw.db[symbol], data)
        except Exception as e:
            logging.error('Write fin | %s | %s | %s', symbol, date, e)
        else:
            logging.warning('Write fin | %s | %s | %s', symbol, date, result)


def write(names=None, start=19910101, end=20291231, how="insert"):
    names = rename(names)
    if names is None:
        mis = None
        f_mis = None
    else:
        mis, f_mis = [], []
        for name in names:
            if name in MI:
                mis.append(name)
            elif name in F_MI:
                f_mis.append(name)

    if not (mis is not None and (len(mis) == 0)):
        write_mi(mis, start, end, how)

    if not (f_mis is not None and (len(f_mis) == 0)):
        write_none_mi(f_mis, start, end, how)


def create(start=19910101, end=20291231, how="insert"):
    table = InstTable(get_collection(variables.InstInfo))
    fi = FutureIndex(get_collection(variables.FUTURE_MI))
    try:
        list_dates = table.future_contract_date_range(inst_type=[101, 102, 103]).applymap(int)
        result = create_commercial_index(start, end, list_dates).rename_axis(int2date).dropna(how='all')
        r = fi.flush(result, how)
        fi.ensure_index()
    except Exception as e:
        logging.error("Create Future MI index | %s | %s | %s | %s", how, start, end, e)
    else:
        logging.warning("Create Future MI index | %s | %s | %s | %s", how, start, end, r)


def contract(names=None, start=19910101, end=20291231, how="update"):
    names = rename(names)
    finder = FutureMIFinder(get_collection(variables.CONTRACTS))
    it = InstTable(get_collection(variables.InstInfo))
    symbols = it.valid_future_symbols(start)
    if names is not None:
        symbols = symbols.loc[names]
    mis = find_daily_mi(",".join(symbols), start, end)
    finder.write(mis, how)


def iter_range(series):
    start = series.index[0]
    end = start
    value = series.iloc[0]
    for date, symbol in series.items():
        if value == symbol:
            end = date
        else:
            yield start, end, value, series.name
            start, end, value = date, date, symbol
    yield start, end, value, series.name


import click


def yesterday():
    return date2num(datetime.now() - timedelta(1))


def today():
    return date2num(datetime.now())


START = click.Option(["-s", "--start"], default=yesterday(), type=click.INT)
END = click.Option(["-e", "--end"], default=today(), type=click.INT)
NAMES = click.Argument(["names"], nargs=-1)
HOW = click.Option(["-h", "--how"], type=click.STRING)
HOW_UPDATE = click.Option(["-h", "--how"], default="update", type=click.STRING)
HOW_INSERT = click.Option(["-h", "--how"], default="insert", type=click.STRING)
COVER = click.Option(['-c', "--cover"], default=False, is_flag=True)


group = click.Group(
    "future",
    commands={
        "create": click.Command("create", callback=create,
                                params=[START, END, HOW_INSERT]),
        "contract": click.Command("contract", callback=contract,
                                  params=[NAMES, START, END, HOW_UPDATE]),
        "check": click.Command("check", callback=check,
                               params=[NAMES, START, END, HOW_UPDATE, COVER]),
        "write": click.Command("check", callback=write,
                               params=[NAMES, START, END, HOW_INSERT])
    }
)