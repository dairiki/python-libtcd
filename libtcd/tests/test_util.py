# -*- coding: utf-8 -*-
"""
"""
from __future__ import absolute_import

from datetime import timedelta
import pytest

@pytest.mark.parametrize("td,expected", [
    (timedelta(), 0),
    (timedelta(hours=1), 60),
    (timedelta(seconds=29.99), 0),
    (timedelta(seconds=30.01), 1),
    (timedelta(seconds=-29.99), 0),
    (timedelta(seconds=-30.01), -1),
    (timedelta(hours=-2), -120),
    ])
def test_timedelta_total_minutes(td, expected):
    from libtcd.util import timedelta_total_minutes
    minutes = timedelta_total_minutes(td)
    assert minutes == expected
