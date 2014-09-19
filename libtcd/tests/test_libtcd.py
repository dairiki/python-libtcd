# -*- coding: utf-8 -*-
"""
"""
from __future__ import absolute_import

from itertools import count, takewhile
from pkg_resources import resource_filename
from shutil import copyfileobj
import tempfile

import pytest
from six.moves import map

from libtcd.util import remove_if_exists

TEST_TCD = resource_filename('libtcd.tests', 'harmonics-initial.tcd')

@pytest.fixture
def test_tcdfile(request):
    from libtcd._libtcd import open_tide_db, close_tide_db, ENCODING
    from libtcd.compat import bytes_

    # Copy original so make sure it doesn't get mutated
    tmpfp = tempfile.NamedTemporaryFile()
    with open(TEST_TCD, "rb") as infp:
        copyfileobj(infp, tmpfp)
    tmpfp.flush()

    def fin():
        close_tide_db()
        tmpfp.close()
    request.addfinalizer(fin)

    open_tide_db(bytes_(tmpfp.name, ENCODING))
    return tmpfp.name

@pytest.fixture
def empty_tcdfile(request):
    from libtcd._libtcd import (
        create_tide_db, close_tide_db, ENCODING,
        c_char_p, c_float32, c_float64, POINTER)
    from libtcd.compat import bytes_

    filename = tempfile.NamedTemporaryFile(delete=False).name
    def fin():
        close_tide_db()
        remove_if_exists(filename)
    request.addfinalizer(fin)

    contituents = (c_char_p * 0)()
    speeds = (c_float64 * 0)()
    equilibriums = epochs = (POINTER(c_float32) * 0)()
    create_tide_db(bytes_(filename, ENCODING), 0, contituents, speeds,
                   1970, 0, equilibriums, epochs)
    return filename

@pytest.fixture(params=['test_tcdfile', 'empty_tcdfile'])
def any_tcdfile(request):
    fixture = request.param
    return request.getfuncargvalue(fixture)


def test_get_tide_db_header(test_tcdfile):
    from libtcd._libtcd import get_tide_db_header
    header = get_tide_db_header()
    assert b'v2.2' in header.version
    assert header.major_rev == 2
    assert header.minor_rev == 2
    assert header.number_of_records == 2
    assert header.start_year == 1970
    assert header.number_of_years == 68

@pytest.mark.parametrize("method,string0,contains", [
    ('get_level_units', b'Unknown', b'knots^2'),
    ('get_dir_units', b'Unknown', b'degrees'),
    ('get_restriction', b'Public Domain', b'DoD/DoD Contractors Only'),
    ('get_country', b'Unknown', b'United States'),
    ('get_legalese', b'NULL', None),
    ('get_datum', b'Unknown', b'Mean Lower Low Water'),
    ('get_tzfile', b'Unknown', b':America/Los_Angeles'),
    ])
def test_get_string(any_tcdfile, method, string0, contains):
    from libtcd import _libtcd
    getter = getattr(_libtcd, method)
    assert getter(0) == string0
    assert getter(-1) == b'Unknown'
    if contains is not None:
        strings = takewhile(lambda s: s != b'Unknown', map(getter, count(1)))
        assert contains in strings
    else:
        assert getter(1) == b'Unknown'

@pytest.mark.parametrize("method,str,expected", [
    ('find_level_units', b'knots^2', 4),
    ('find_dir_units', b'degrees', 2),
    ('find_restriction', b'Non-commercial use only', 2),
    ('find_country', b'United States', 224),
    ('find_legalese', b'NULL', 0),
    ('find_datum', b'Mean Lower Low Water', 3),
    ('find_tzfile', b':America/Los_Angeles', 115),
    ])
def test_find_string(test_tcdfile, method, str, expected):
    from libtcd import _libtcd
    finder = getattr(_libtcd, method)
    assert finder(str) == expected
    assert finder(b'does not exist') == -1

@pytest.mark.parametrize(
    "table", ['restriction', 'country', 'legalese', 'datum', 'tzfile'])
def test_add_string(any_tcdfile, table):
    from libtcd import _libtcd
    get = getattr(_libtcd, 'get_%s' % table)
    find = getattr(_libtcd, 'find_%s' % table)
    add = getattr(_libtcd, 'add_%s' % table)
    s = b'some string'
    i = add(s)
    assert i > 0
    assert get(i) == s
    j = add(s)
    assert j != i
    assert get(j) == s

@pytest.mark.parametrize(
    "table", ['restriction', 'country', 'legalese', 'datum', 'tzfile'])
def test_find_or_add_string(any_tcdfile, table):
    from libtcd import _libtcd
    get = getattr(_libtcd, 'get_%s' % table)
    find = getattr(_libtcd, 'find_%s' % table)
    find_or_add = getattr(_libtcd, 'find_or_add_%s' % table)
    s = b'does not exist'
    assert find(s) == -1
    i = find_or_add(s)
    assert i > 0
    assert get(i) == s
    assert find(s) == i
    assert find_or_add(s) == i

def test_level_units(any_tcdfile):
    from libtcd._libtcd import get_level_units, get_tide_db_header
    header = get_tide_db_header()
    level_units = map(get_level_units, range(header.level_unit_types))
    assert list(level_units) == [
        b'Unknown', b'feet', b'meters', b'knots', b'knots^2']

def test_dir_units(any_tcdfile):
    from libtcd._libtcd import get_dir_units, get_tide_db_header
    header = get_tide_db_header()
    dir_units = map(get_dir_units, range(header.dir_unit_types))
    assert list(dir_units) == [b'Unknown', b'degrees true', b'degrees']

def test_get_partial_tide_record(test_tcdfile):
    from libtcd._libtcd import get_partial_tide_record
    header = get_partial_tide_record(0)
    assert header.name.startswith(b'Alameda,')
    assert get_partial_tide_record(42) is None

def test_get_next_partial_tide_record(test_tcdfile):
    from libtcd._libtcd import (
        get_partial_tide_record,
        get_next_partial_tide_record,
        )
    headers = [ get_partial_tide_record(0) ]
    next_header = get_next_partial_tide_record()
    while next_header is not None:
        headers.append(next_header)
        next_header = get_next_partial_tide_record()
    assert len(headers) == 2
