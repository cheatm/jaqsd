from jaqsd import conf
from jaqsd.table import group as table
from jaqsd.collector.ins_info import group as instrument
from jaqsd.indicator import group as indicator
from jaqsd.finance import group as finance
from jaqsd.daily import group as daily_index
from jaqsd.collector.future import group as future
from jaqsd.init import group as init
import click


conf.init()


group = click.Group(
    "jaqsd",
    {"instrument": instrument,
     "indicator": indicator,
     "finance": finance,
     "daily_index": daily_index,
     "table": table,
     "future": future,
     "init": init}
)

if __name__ == '__main__':
    group()