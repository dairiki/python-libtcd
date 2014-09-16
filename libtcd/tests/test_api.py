# -*- coding: utf-8 -*-
"""
"""
from __future__ import absolute_import

from pkg_resources import resource_filename

import pytest


@pytest.fixture
def test_tcd():
    from libtcd.api import Tcd
    filename = resource_filename('libtcd.tests', 'harmonics-initial.tcd')
    return Tcd.open(filename)

def test_constituents(test_tcd):
    assert len(test_tcd.constituents) == 173

def test_len(test_tcd):
    assert len(test_tcd) == 2

def test_iter(test_tcd):
    stations = list(test_tcd)
    assert [s.name for s in stations] \
           == ["Alameda, San Francisco Bay, California"] * 2
    assert len(stations[0].coefficients) == 32
