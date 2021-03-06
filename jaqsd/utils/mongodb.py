from pymongo import InsertOne, UpdateOne
import pandas as pd
import six


def iter_insert(data):
    if data.index.name is not None:
        data = data.reset_index()
    for key, values in data.iterrows():
        yield make_insert(values)


def make_insert(series):
    dct = series.dropna().to_dict()
    return InsertOne(dct)


def insert(collection, data):
    return collection.bulk_write(list(iter_insert(data))).inserted_count


def iter_update(data, **kwargs):
    if isinstance(data, pd.DataFrame):
        if isinstance(data.index, pd.MultiIndex):
            for key, values in data.reset_index().iterrows():
                yield make_update(values, data.index.names, **kwargs)
        else:
            for key, values in data.reset_index().iterrows():
                yield make_update(values, [data.index.name], **kwargs)


def make_update(series, index, how="$set", upsert=True, **kwargs):
    return UpdateOne({i: series[i] for i in index}, {how: series.dropna().to_dict()}, upsert=upsert)


def update(collection, data, **kwargs):
    result = collection.bulk_write(list(iter_update(data, **kwargs)))
    return result.matched_count, result.upserted_count


def append(collection, data):
    return update(collection, data, how='$setOnInsert')


METHODS = {"insert": iter_insert,
           "update": iter_update}


WRITE_METHODS = {"insert": insert,
                 "update": update,
                 "append": append}


def read(collection, index, start=None, end=None, fields=None, filters=None):
    if isinstance(filters, dict):
        filters = filters.copy()
    else:
        filters = {}
    if start:
        filters[index] = {"$gte": start}
    if end:
        filters.setdefault(index, {})["$lte"] = end

    prj = {"_id": 0}
    if isinstance(fields, six.string_types):
        prj.update(dict.fromkeys(fields.split(","), 1))
        prj[index] = 1
    elif fields is not None:
        prj.update(dict.fromkeys(fields, 1))
        prj[index] = 1

    data = pd.DataFrame(list(collection.find(filters, prj)))
    return data.set_index(index)


class IndexMongodbMixin(object):

    def __init__(self, collection, index="trade_date"):
        self.collection = collection
        self.index = index

    def _create(self, table, how="insert"):
        method = WRITE_METHODS[how]
        return method(self.collection, table)

    def _read(self, start=None, end=None, fields=None):
        return read(self.collection, self.index, start, end, fields)

    def _update(self, table):
        return update(self.collection, table, upsert=False)


from jaqsd import conf


def get_collection(view):
    db_col = conf.get_col(view)
    db, col = db_col.split(".", 1)
    client = conf.get_client()
    return client[db][col]


def get_db(name):
    db = conf.get_db(name)
    client = conf.get_client()
    return client[db]


class SyncTable(object):

    def __init__(self, collection, table):
        self.collection = collection
        self.table = table

    def clear(self):
        return self.collection.delete_many({})

    def sync(self, start=None, end=None):
        t = self.table.loc[start:end]
        return update(self.collection, t)

    def create(self):
        result = insert(self.collection, self.table)
        self.collection.create_index(self.table.index.name)
        return result

