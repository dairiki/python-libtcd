# -*- coding: utf-8 -*-
"""
"""
from __future__ import absolute_import

import errno
from shutil import copyfileobj
import tempfile
from pkg_resources import resource_filename
import os

import pytest

TCD_FILENAME = resource_filename('libtcd.tests', 'harmonics-initial.tcd')

@pytest.fixture
def test_tcd():
    from libtcd.api import Tcd
    return Tcd.open(TCD_FILENAME)

@pytest.fixture
def new_tcd():
    from libtcd.api import Tcd, Constituent, NodeFactors, NodeFactor
    tmpfile = tempfile.NamedTemporaryFile()
    c = Constituent('J1', 15.5854433, NodeFactors(1970, [NodeFactor(1.0, 2.0)]))
    constituents = {c.name: c}
    return Tcd(tmpfile.name, constituents)

@pytest.fixture
def temp_tcd(request):
    from libtcd.api import Tcd
    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    with open(TCD_FILENAME, 'rb') as infp:
        copyfileobj(infp, tmpfile)
    tmpfile.flush()

    def finalizer():
        try:
            os.unlink(tmpfile.name)
        except OSError as ex:
            if ex.errno != errno.ENOENT:
                raise
    request.addfinalizer(finalizer)

    return Tcd.open(tmpfile.name)

def check_not_locked():
    from libtcd.api import _lock
    assert _lock.acquire(False)
    _lock.release()

def test_get_current_database(test_tcd):
    from libtcd.api import get_current_database
    with test_tcd:
        assert get_current_database() == test_tcd

def test_enter_failure(temp_tcd):
    from libtcd import _libtcd
    os.unlink(temp_tcd.filename)
    temp_tcd.close()
    with pytest.raises(_libtcd.Error):
        temp_tcd.__enter__()
    check_not_locked()

def test_constituents(new_tcd):
    constituents = new_tcd.constituents
    assert len(constituents) == 1
    assert list(constituents)[0] == 'J1'
    assert constituents['J1'].speed == 15.5854433

def test_len(test_tcd):
    assert len(test_tcd) == 2

def test_getitem(test_tcd):
    assert test_tcd[0].name == u"Alameda, San Francisco Bay, California"
    with pytest.raises(IndexError):
        test_tcd[-1]
    with pytest.raises(IndexError):
        test_tcd[len(test_tcd)]

def test_setitem(temp_tcd):
    station = temp_tcd[1]
    station.name = u"Göober"
    temp_tcd[0] = station
    assert temp_tcd[0].name == u"Göober"

def test_delitem(temp_tcd):
    ilen = len(temp_tcd)
    del temp_tcd[0]
    assert len(temp_tcd) == ilen - 1

def test_append(temp_tcd):
    tcd = temp_tcd
    ilen = len(tcd)
    tcd.append(tcd[0])
    assert len(temp_tcd) == ilen + 1

def test_iter(test_tcd):
    stations = list(test_tcd)
    assert [s.name for s in stations] \
           == ["Alameda, San Francisco Bay, California"] * 2
    assert len(stations[0].coefficients) == 32

def test_restrictions(temp_tcd):
    from libtcd.api import _restrictions
    with temp_tcd:
        assert list(_restrictions) == [
            u'Public Domain',
            u'DoD/DoD Contractors Only',
            u'Non-commercial use only']
        assert _restrictions.find_or_add(u'Fü') == 3
        assert _restrictions.find_or_add(u'Fü') == 3

def test_tzfiles(temp_tcd):
    from libtcd.api import _tzfiles
    with temp_tcd:
        assert len(_tzfiles) == 406
        assert _tzfiles[0] == u'Unknown'
        assert u':America/Los_Angeles' in _tzfiles
        assert _tzfiles.find_or_add(u'Fü') == 406
        assert _tzfiles.find_or_add(u'Fü') == 406

def test_countries(temp_tcd):
    from libtcd.api import _countries
    with temp_tcd:
        assert len(_countries) == 240
        assert _countries[0] == u'Unknown'
        assert u'United States' in _countries
        assert _countries.find_or_add(u'Fü') == 240
        assert _countries.find_or_add(u'Fü') == 240

def test_datums(temp_tcd):
    from libtcd.api import _datums
    with temp_tcd:
        assert _datums[0] == u'Unknown'
        assert u'Mean Lower Low Water' in _datums
        assert _datums.find_or_add(u'Fü') == 61
        assert _datums.find_or_add(u'Fü') == 61

def test_legaleses(temp_tcd):
    from libtcd.api import _legaleses
    with temp_tcd:
        assert list(_legaleses) == [u'NULL']
        assert _legaleses.find_or_add(u'Fü') == 1
        assert _legaleses.find_or_add(u'Fü') == 1

def test_level_units(test_tcd):
    from libtcd.api import _level_units
    with test_tcd:
        assert list(_level_units) == [
            u'Unknown', u'feet', u'meters', u'knots', u'knots^2']

def test_dir_units(test_tcd):
    from libtcd.api import _dir_units
    with test_tcd:
        assert list(_dir_units) == [
            u'Unknown', u'degrees true', u'degrees']

def test_default_restrictions(new_tcd):
    from libtcd.api import _restrictions
    with new_tcd:
        assert list(_restrictions) == [
            u'Public Domain',
            u'DoD/DoD Contractors Only']

def test_default_tzfiles(new_tcd):
    from libtcd.api import _tzfiles
    with new_tcd:
        assert _tzfiles[0] == u'Unknown'

def test_default_countries(new_tcd):
    from libtcd.api import _countries
    with new_tcd:
        assert _countries[0] == u'Unknown'

def test_default_datums(new_tcd):
    from libtcd.api import _datums
    with new_tcd:
        assert _datums[0] == u'Unknown'

def test_default_legaleses(new_tcd):
    from libtcd.api import _legaleses
    with new_tcd:
        assert list(_legaleses) == [u'NULL']

def test_default_level_units(new_tcd):
    from libtcd.api import _level_units
    with new_tcd:
        assert list(_level_units) == [
            u'Unknown', u'feet', u'meters', u'knots', u'knots^2']

def test_default_dir_units(new_tcd):
    from libtcd.api import _dir_units
    with new_tcd:
        assert list(_dir_units) == [
            u'Unknown', u'degrees true', u'degrees']
