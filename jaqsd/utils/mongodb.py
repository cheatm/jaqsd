from pymongo import InsertOne, UpdateOne
import pandas as pd
import six


def iter_insert(data):
    if isinstance(data, pd.DataFrame):
        for key, values in data.iterrows():
            yield make_insert(values, data.index.name)


def make_insert(series, index):
    dct = series.dropna().to_dict()
    dct[index] = series.name
    return InsertOne(dct)


def insert(collection, data):
    return collection.bulk_write(list(iter_insert(data))).inserted_count


def iter_update(data, **kwargs):
    if isinstance(data, pd.DataFrame):
        for key, values in data.iterrows():
            yield make_update(values, data.index.name)


def make_update(series, index, upsert=True, **kwargs):
    return UpdateOne({index: series.name}, {"$set": series.dropna().to_dict()}, upsert=upsert)


def update(collection, data, **kwargs):
    result = collection.bulk_write(list(iter_update(data, **kwargs)))
    return result.matched_count, result.upserted_count


METHODS = {"insert": iter_insert,
           "update": iter_update}


WRITE_METHODS = {"insert": insert,
                 "update": update}


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
    elif fields is not None:
        prj.update(dict.fromkeys(fields, 1))
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


# from jaqsd.utils.table import BaseIndexTable
#
#
# class MongodbTable(BaseIndexTable, IndexMongodbMixin):
#
#     def __init__(self, collection, index="trade_date"):
#         IndexMongodbMixin.__init__(self, collection, index)
#
#     def get(self, start=None, end=None, fields=None):
#         try:
#             return self._read(start, end, fields)
#         except Exception as e:
#             return pd.DataFrame()
#
#     def find(self, value, start=None, end=None, fields=None, axis=0):
#         table = self.get(start, end, fields)
#         if axis == 0:
#             iterable = table.iterrows()
#         else:
#             iterable = table.iteritems()
#
#         for name, series in iterable:
#             yield name, series[series==0].index
#
#     def fill(self, index, column, value):
#         return self.collection.update_one({self.index: index}, {"$set": {column: value}})
#
#

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

