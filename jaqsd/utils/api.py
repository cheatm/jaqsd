import jaqsd.conf as conf
from jaqs.data import DataApi


def get_api():
    api = DataApi()
    api.login(conf.username(), conf.password())
    return api



