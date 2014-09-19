# -*- coding: utf-8 -*-
"""
"""
from __future__ import absolute_import

def timedelta_total_minutes(td, strict=False):
    minutes, seconds = divmod(td.days * 24 * 3600 + td.seconds, 60)
    if seconds or td.microseconds:
        if strict:
            raise ValueError("%r is not an integral multiple of 60 seconds", td)
        if seconds > 30 or (seconds == 30 and td.microseconds > 0):
            minutes += 1
    return minutes
