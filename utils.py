#!/usr/bin/python3
__author__ = 'ziyan.yin'

from threading import Lock
from typing import Dict

from unit import units

locks: Dict[str, Lock] = dict()


def synchronized(func):
    key = f"{repr(func)}"
    if key not in locks:
        locks[key] = Lock()

    def wrapper(*args, **kwargs):
        with locks[key]:
            return func(*args, **kwargs)

    return wrapper


def register(cls):
    module = '.'.join(cls.__module__.split('.')[1:])
    if module not in units:
        units[module] = dict()
    units[module][cls.__name__] = cls
    return cls


def priority(level: int):
    if not 0 < level < 5:
        level = 1

    def wrapper(cls):
        cls.level = level
        return cls

    return wrapper


def get_unit(router: str = 'common', method: str = ''):
    if router:
        if router in units and method in units[router]:
            return True, units[router][method]
        else:
            return False, 'can not find service {0}[{1}]'.format(router, method)
    return False, ''
