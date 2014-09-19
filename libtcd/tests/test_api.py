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
