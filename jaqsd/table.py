from jaqsd.utils.mongodb import SyncTable
from jaqsd.utils.tool import logger, START_STR, END_TODAY_STR, day_shift
from jaqsd import conf
import pandas as pd
import click
import os


def iterlize(key, default=None):
    if default is None:
        default = lambda: list()

    def wrapper(func):
        def wrapped(*args, **kwargs):
            results = []
            keys = kwargs.get(key, [])
            if len(keys) == 0:
                keys = default()

            for k in keys:
                kwargs[key] = k
                result = func(*args, **kwargs)
                results.append((k, result))
            return results
        return wrapped
    return wrapper


def read(file_name):
    return pd.read_excel(file_name, index_col="trade_date", dtype=float).rename_axis(lambda s: str(s)).rename_axis(dot2line, 1)


def dot2line(s):
    return s.replace(".", "_")


def get_st(name):
    st = conf.get_tables()[name]
    db, col = st["col"].split(".", 1)
    client = conf.get_client()
    file_name = os.path.join(conf.CONF_DIR, st["file"])
    t = read(file_name)
    return SyncTable(client[db][col], t)


NAME = click.argument("name", nargs=-1)
CLEAN = click.option("-c", "clean", is_flag=True, default=False)


@click.command("create")
@NAME
@CLEAN
@iterlize("name", lambda: conf.get_tables().keys())
@logger("create", "name")
def create(name, clean=False):
    st = get_st(name)
    if clean:
        st.clear()
    return st.create()


@click.command("sync")
@END_TODAY_STR
@click.option("-s", "--start", default=str(day_shift(14)), type=click.STRING)
@NAME
@iterlize("name", lambda: conf.get_tables().keys())
@logger("sync", "name", "start", "end")
def sync(name, start=None, end=None):
    st = get_st(name)
    return st.sync(start, end)


@click.command("clear")
@NAME
@iterlize("name", lambda: conf.get_tables().keys())
def clear(name):
    st = get_st(name)
    st.clear()


group = click.Group(
    "group",
    {"sync": sync,
     "create": create,
     "clear": clear}
)

