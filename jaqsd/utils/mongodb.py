from pymongo import InsertOne, UpdateOne
import pandas as pd


def iter_insert(data):
    if isinstance(data, pd.DataFrame):
        for key, values in data.iterrows():
            yield make_insert(values, data.index.name)


def make_insert(series, index):
    dct = series.dropna().to_dict()
    dct[index] = series.name
    return InsertOne(dct)


def iter_update(data):
    if isinstance(data, pd.DataFrame):
        for key, values in data.iterrows():
            yield make_update(values, data.index.name)


def make_update(series, index):
    return UpdateOne({index: series.name}, series.dropna().to_dict())


METHODS = {"insert": iter_insert,
           "update": iter_update}