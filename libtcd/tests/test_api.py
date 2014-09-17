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
    constituents = test_tcd.constituents
    assert len(constituents) == 173
    assert list(constituents)[0] == 'J1'
    assert constituents['J1'].speed == 15.5854433

def test_len(test_tcd):
    assert len(test_tcd) == 2

def test_iter(test_tcd):
    stations = list(test_tcd)
    assert [s.name for s in stations] \
           == ["Alameda, San Francisco Bay, California"] * 2
    assert len(stations[0].coefficients) == 32

def test_restrictions(test_tcd):
    assert list(test_tcd.restrictions) == [
        u'Public Domain',
        u'DoD/DoD Contractors Only',
        u'Non-commercial use only']

def test_tzfiles(test_tcd):
    tzfiles = test_tcd.tzfiles
    assert tzfiles[0] == u'Unknown'
    assert u':America/Los_Angeles' in tzfiles

def test_countries(test_tcd):
    countries = test_tcd.countries
    assert countries[0] == u'Unknown'
    assert u'United States' in countries

def test_datums(test_tcd):
    datums = test_tcd.datums
    assert datums[0] == u'Unknown'
    assert u'Mean Lower Low Water' in datums

def test_legaleses(test_tcd):
    assert list(test_tcd.legaleses) == [u'NULL']

def test_level_units(test_tcd):
    assert list(test_tcd.level_units) == [
        u'Unknown', u'feet', u'meters', u'knots', u'knots^2']

def test_dir_units(test_tcd):
    assert list(test_tcd.dir_units) == [
        u'Unknown', u'degrees true', u'degrees']
