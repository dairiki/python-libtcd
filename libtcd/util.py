# -*- coding: utf-8 -*-
"""
"""
from __future__ import absolute_import

import errno
import os


def timedelta_total_minutes(td, strict=False):
    """ Convert/round a :cls:`timedelta` to an integral number of minutes.

    If ``strict`` is true than raises :exc:`ValueError` if the
    :cls:`~datetime.timedelta` is not a integral multiple of 60 seconds.

    """
    minutes, seconds = divmod(td.days * 24 * 3600 + td.seconds, 60)
    if seconds or td.microseconds:
        if strict:
            raise ValueError(
                "%r is not an integral multiple of 60 seconds", td)
        if seconds > 30 or (seconds == 30 and td.microseconds > 0):
            minutes += 1
    return minutes


def remove_if_exists(filename):
    """ Remove file.

    This is like :func:`os.remove` (or :func:`os.unlink`), except that no
    error is raised if the file does not exist.

    """
    try:
        os.unlink(filename)
    except OSError as ex:
        if ex.errno != errno.ENOENT:
            raise


class reify(object):
    """ Use as a class method decorator.  It operates almost exactly like the
    Python ``@property`` decorator, but it puts the result of the method it
    decorates into the instance dict after the first call, effectively
    replacing the function it decorates with an instance variable.  It is, in
    Python parlance, a non-data descriptor.  An example:

    Ripped verbatim from :class:`pyramid.decorator.reify`.

    """
    def __init__(self, wrapped):
        self.wrapped = wrapped
        try:
            self.__doc__ = wrapped.__doc__
        except:  # pragma: no cover
            pass

    def __get__(self, inst, objtype=None):
        if inst is None:
            return self
        val = self.wrapped(inst)
        setattr(inst, self.wrapped.__name__, val)
        return val
