"""
Simple unit conversions.
"""

import numbers


def milli(val):
    assert isinstance(val, numbers.Number)

    return val / 1000.


def kilo(val):
    assert isinstance(val, numbers.Number)
    assert val >= 0
    return 1000 * val


def mega(val):
    assert isinstance(val, numbers.Number)
    return 1000 * kilo(val)


def giga(val):
    assert isinstance(val, numbers.Number)
    return 1000 * mega(val)

