from jaqsd.utils.api import get_api
from jaqsd.utils.mongodb import update
from jaqsd import conf
from pymongo.database import Database
from datetime import datetime


conf.init()

api = get_api()


def get_instruments():
    data, msg = api.query("jz.instrumentInfo", "inst_type=1&market=SZ,SH&status=1")
    if msg == "0,":
        return ",".join(data["symbol"])
    else:
        raise Exception(msg)
    

def search(db):
    assert isinstance(db, Database)
    targets = {}
    for name in db.collection_names():
        doc = db[name].find_one(None, sort=[('datetime', -1)])
        if doc:
            yield doc["datetime"], name


def defold(symbol):
    if symbol.endswith(".XSHG"):
        return symbol[:-4] + "SH"
    else:
        return symbol[:-4] + "SZ"


def get_data(start, symbols):
    s = start.year*10000 + start.month*100 + start.day
    now = datetime.now()

    data, msg = api.daily(symbols, s, 20291231)
    if msg == "0,":
        r = data[data["volume"]>0][["open", "high", "low", "close", "turnover", "volume", "trade_date"]]
        r.set_index("trade_date", inplace=True)
        r.index.name = "datetime"
        r.index = r.index.map(simple2datetime)
        return r
    else:
        raise Exception(msg)


def simple2datetime(t):
    return datetime.strptime(str(t), "%Y%m%d").replace(hour=15)


def join(symbols):
    return ",".join(map(defold, symbols))


def main():
    run()


import logging


def run():
    from pymongo import MongoClient

    client = conf.get_client()
    db = client[conf.get_db("daily")]
    for date, symbol in search(db):
        try:
            data = get_data(date, defold(symbol))
        except Exception as e:
            logging.error("daily | %s | %s | %s", symbol, date, e)
        else:
            if len(data.index):
                r = update(db[symbol], data)
                logging.warning("daily | %s | %s | %s", symbol, date, r)
        

if __name__ == '__main__':
    main()