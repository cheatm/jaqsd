from jaqsd import conf, finance, indicator
from jaqsd.utils.tool import START, END, APPEND, COVER, FORCE
import click
import os


@click.command("lb_daily")
@START
@END
@APPEND
@COVER
@FORCE
def init_lb_daily(start, end, append, cover, force=False):
    if not os.path.exists(conf.get_file_path("lb_daily")) or force:
        finance.create.callback(start, end, append)
    finance.check.callback([], start, end, cover)


@click.command("indicator")
@START
@END
@APPEND
@COVER
@FORCE
def init_daily_indicator(start, end, append, cover, force=False):
    if not os.path.exists(conf.get_file_path("lb_daily")) or force:
        indicator.create.callback(start, end, append)
    indicator.check.callback([], start, end, cover)


group = click.Group("init",
                    commands={"indicator": init_daily_indicator,
                              "lb_daily": init_lb_daily})


if __name__ == '__main__':
    conf.init()
    group()