from functools import wraps


class Singleton(object):

    def __init__(self):
        self.cache = {}

    def __call__(self, name):
        def wrapper(func):
            @wraps(func)
            def wrapped(*args, **kwargs):
                try:
                    return self.cache[name]
                except KeyError:
                    value = func(*args, **kwargs)
                    self.cache[name] = value
                    return value
            return wrapped
        return wrapper

    def delete(self, name):
        del self.cache[name]


single = Singleton()