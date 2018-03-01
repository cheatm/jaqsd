from jaqsd.conf import load

codes = []
FILE = "indexes.yml"


def init():
    globals()["codes"] = load(FILE, list)

