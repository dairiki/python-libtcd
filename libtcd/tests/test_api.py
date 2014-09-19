# -*- coding: utf-8 -*-
"""
"""
from __future__ import absolute_import

from ctypes import c_float
import datetime
from shutil import copyfileobj
import tempfile
from pkg_resources import resource_filename
import os

import pytest
from six import binary_type, integer_types

from libtcd.compat import OrderedDict
from libtcd.util import remove_if_exists

TCD_FILENAME = resource_filename('libtcd.tests', 'harmonics-initial.tcd')

@pytest.fixture
def test_tcd():
    from libtcd.api import Tcd
    return Tcd.open(TCD_FILENAME)

@pytest.fixture
def dummy_constituents():
    from libtcd.api import Constituent, NodeFactors, NodeFactor
    c = Constituent('J1', 15.5854433,
                    NodeFactors(1970, [NodeFactor(1.0, 2.0)]))
    constituents = {c.name: c}
    return constituents

@pytest.fixture
def dummy_refstation(dummy_constituents):
    from libtcd.api import Coefficient, ReferenceStation
    return ReferenceStation(
        name=u'Somewhere',
        coefficients=[
            Coefficient(13.0, 42.0, dummy_constituents['J1']),
            ])

@pytest.fixture
def dummy_substation(dummy_refstation):
    from libtcd.api import SubordinateStation
    return SubordinateStation(
        name=u'Somewhere Else',
        reference_station=dummy_refstation)

@pytest.fixture
def new_tcd(dummy_constituents):
    from libtcd.api import Tcd, Constituent, NodeFactors, NodeFactor
    tmpfile = tempfile.NamedTemporaryFile()
    return Tcd(tmpfile.name, dummy_constituents)

@pytest.fixture
def temp_tcd(request):
    from libtcd.api import Tcd

    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    with open(TCD_FILENAME, 'rb') as infp:
        copyfileobj(infp, tmpfile)
    tmpfile.flush()

    def fin():
        remove_if_exists(tmpfile.name)
    request.addfinalizer(fin)

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
        test_tcd[100000]
    with pytest.raises(IndexError):
        test_tcd[len(test_tcd)]
    assert test_tcd[-1].record_number == len(test_tcd) - 1

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

def test_append_refstation(new_tcd, dummy_refstation):
    tcd = new_tcd
    tcd.append(dummy_refstation)
    assert len(tcd) == 1

def test_append_substation(new_tcd, dummy_substation):
    tcd = new_tcd
    tcd.append(dummy_substation)
    assert len(tcd) == 2

def test_iter(test_tcd):
    stations = list(test_tcd)
    assert [s.name for s in stations] \
           == ["Alameda, San Francisco Bay, California"] * 2
    assert len(stations[0].coefficients) == 32

@pytest.mark.parametrize("seconds,expected", [
    (0, '0:00'),
    (3600, '+01:00'),
    (7229.9, '+02:00'),
    (7230.1, '+02:01'),
    (-3629, '-01:00'),
    ])
def test_timeoffset(seconds, expected):
    from libtcd.api import timeoffset
    offset = timeoffset(seconds=seconds)
    assert str(offset) == expected

class attr_descriptor_test_base(object):

    @pytest.fixture
    def tcd(self):
        return 'ignored'

    @pytest.fixture
    def station(self, descriptor, values):
        name = descriptor.name
        packed, unpacked = values
        class MockStation(object):
            def __init__(self, unpacked=0):
                setattr(self, name, unpacked)
        return MockStation(unpacked)

    @pytest.fixture
    def rec(self, descriptor, values):
        from libtcd._libtcd import TIDE_RECORD
        packed_name = descriptor.packed_name
        packed, unpacked = values
        rec = TIDE_RECORD()
        setattr(rec, packed_name, packed)
        return rec

    @pytest.fixture
    def values(self):
        packed = unpacked = 42
        return packed, unpacked

    def test_pack(self, descriptor, tcd, station, values):
        packed_name = descriptor.packed_name
        packed, unpacked = values
        assert list(descriptor.pack(tcd, station)) \
               == [(packed_name, packed)]

    def test_unpack(self, descriptor, tcd, rec, values):
        name = descriptor.name
        packed, unpacked = values
        assert list(descriptor.unpack(tcd, rec)) == [(name, unpacked)]

    def test_unpack_value(self, descriptor, tcd, values):
        packed, unpacked = values
        assert descriptor.unpack_value(tcd, packed) == unpacked

    def test_pack_value(self, descriptor, tcd, values):
        packed, unpacked = values
        assert descriptor.pack_value(tcd, unpacked) == packed

class Test_string_table(attr_descriptor_test_base):
    @pytest.fixture
    def descriptor_class(self):
        from libtcd.api import _string_table
        return _string_table

    @pytest.fixture
    def descriptor(self, descriptor_class, monkeypatch):
        from libtcd import _libtcd

        table = [
            b'Unknown',
            u'fü'.encode('iso-8859-1'),
            ]

        def get(i):
            assert isinstance(i, integer_types)
            if 0 <= i < len(table):
                return table[i]
            else:
                return b'Unknown'

        def find(s):
            assert isinstance(s, binary_type)
            try:
                return table.index(s)
            except ValueError:
                return -1

        def find_or_add(s):
            assert isinstance(s, binary_type)
            i = find(s)
            if i < 0:
                i = len(table)
                table.append(s)
            return i

        monkeypatch.setattr(_libtcd, 'get_tzfile', get, raising=False)
        monkeypatch.setattr(_libtcd, 'find_tzfile', find, raising=False)
        monkeypatch.setattr(_libtcd, 'find_or_add_tzfile', find_or_add,
                            raising=False)
        return descriptor_class('tzfile')

    @pytest.fixture
    def values(self):
        packed = 1
        unpacked = u'fü'
        return packed, unpacked

    def test_unpack_value_unknown(self, descriptor, tcd):
        assert descriptor.unpack_value(tcd, 0) == u'Unknown'
        assert descriptor.unpack_value(tcd, 2) == u'Unknown'
        assert descriptor.unpack_value(tcd, -1) == u'Unknown'

    def test_pack_value_none(self, descriptor, tcd):
        assert descriptor.pack_value(tcd, None) == 0

    def test_pack_value_unknonw(self, descriptor, tcd):
        assert descriptor.pack_value(tcd, u'Unknown') == 0

    def test_pack_unknown_value(self, descriptor, tcd):
        assert descriptor.pack_value(tcd, u'missing') == 2
        assert descriptor.unpack_value(tcd, 2) == u'missing'

class Test_string_enum(Test_string_table):
    @pytest.fixture
    def descriptor_class(self):
        from libtcd.api import _string_enum
        return _string_enum

    def test_pack_unknown_value(self, descriptor, tcd):
        with pytest.raises(ValueError):
            descriptor.pack_value(tcd, u'missing')

class Test_string(attr_descriptor_test_base):
    @pytest.fixture
    def descriptor(self):
        from libtcd.api import _string
        return _string('name')

    @pytest.fixture
    def values(self):
        unpacked = u'Göober'
        packed = unpacked.encode('iso-8859-1')
        return packed, unpacked

    # FIXME: should this pass?
    #def test_pack_value_none(self, descriptor, tcd):
    #    assert descriptor.pack_value(tcd, None) == b''

class Test_date(attr_descriptor_test_base):
    @pytest.fixture
    def descriptor(self):
        from libtcd.api import _date
        return _date('date_imported')

    @pytest.fixture
    def values(self):
        unpacked = datetime.date(2001, 2, 3)
        packed = 20010203
        return packed, unpacked

class Test_time_offset(attr_descriptor_test_base):
    @pytest.fixture
    def descriptor(self):
        from libtcd.api import _time_offset
        return _time_offset('zone_offset')

    @pytest.fixture
    def values(self):
        unpacked = -datetime.timedelta(hours=9, minutes=30)
        packed = -930
        return packed, unpacked

    def test_unpack_bad_value(self, descriptor, tcd):
        with pytest.raises(AssertionError):
            descriptor.unpack_value(tcd, 60)

    def test_pack_value_none(self, descriptor, tcd):
        assert descriptor.pack_value(tcd, None) == 0

class Test_direction(attr_descriptor_test_base):
    @pytest.fixture
    def descriptor(self):
        from libtcd.api import _direction
        return _direction('min_direction')

    def test_unpack_value_none(self, descriptor, tcd):
        assert descriptor.unpack_value(tcd, 361) is None

    # FIXME: should this pass?
    #def test_pack_value_none(self, descriptor, tcd):
    #    assert descriptor.pack_value(tcd, None) == 361

    def test_pack_value_raises_value_error(self, descriptor, tcd):
        with pytest.raises(ValueError):
            descriptor.pack_value(tcd, 361)

class Test_xfields(attr_descriptor_test_base):
    @pytest.fixture
    def descriptor(self):
        from libtcd.api import _xfields
        return _xfields('xfields')

    @pytest.fixture
    def values(self):
        packed = (b'a:b\n'
                  b' b2\n'
                  b'c: d \n')
        unpacked = OrderedDict([
            ('a', 'b\nb2'),
            ('c', ' d '),
            ])
        return packed, unpacked

    def test_unpack_value_ignores_cruft(self, descriptor, tcd, values):
        packed, unpacked = values
        assert descriptor.unpack_value(tcd, packed + b'\nfoo\n') == unpacked

class Test_record_number(attr_descriptor_test_base):
    @pytest.fixture
    def descriptor(self):
        from libtcd.api import _record_number
        return _record_number('record_number')

    def test_pack(self, descriptor, tcd):
        assert list(descriptor.pack(tcd, 42)) == []

class Test_record_type(attr_descriptor_test_base):
    @pytest.fixture
    def descriptor(self):
        from libtcd.api import _record_type
        return _record_type('record_type')

    def test_unpack(self, descriptor, rec, tcd):
        assert list(descriptor.unpack(tcd, rec)) == []

    def test_pack(self, descriptor, tcd):
        from libtcd.api import ReferenceStation, SubordinateStation
        refstation = ReferenceStation('ref', [])
        substation = SubordinateStation('sub', refstation)

        assert list(descriptor.pack(tcd, refstation)) == [('record_type', 1)]
        assert list(descriptor.pack(tcd, substation)) == [('record_type', 2)]

class Test_coordinates(attr_descriptor_test_base):
    @pytest.fixture
    def descriptor(self):
        from libtcd.api import _coordinates
        return _coordinates('coordinates')

    def test_unpack(self, descriptor, tcd, rec):
        rec.latitude = 0.0
        rec.longitude = 1.0
        assert dict(descriptor.unpack(tcd, rec)) == {
            'latitude': 0.0,
            'longitude': 1.0,
            }

    def test_unpack_none(self, descriptor, tcd, rec):
        rec.latitude = 0.0
        rec.longitude = 0.0
        assert dict(descriptor.unpack(tcd, rec)) == {
            'latitude': None,
            'longitude': None,
            }

    def test_pack(self, descriptor, tcd, station):
        station.latitude = 0.0
        station.longitude = 1.0
        assert dict(descriptor.pack(tcd, station)) == {
            'latitude': 0.0,
            'longitude': 1.0,
            }
    def test_pack_none(self, descriptor, tcd, station):
        station.latitude = station.longitude = None
        assert dict(descriptor.pack(tcd, station)) == {
            'latitude': 0.0,
            'longitude': 0.0,
            }

class Test_coefficients(attr_descriptor_test_base):
    @pytest.fixture
    def descriptor(self):
        from libtcd.api import _coefficients
        return _coefficients('coefficients')

    @pytest.fixture
    def tcd(self, new_tcd):
        return new_tcd

    def test_unpack(self, descriptor, tcd, rec):
        from libtcd.api import Coefficient

        rec.amplitude = (c_float * 255)(1.5)
        rec.epoch = (c_float * 255)(42.0)
        constituents = list(tcd.constituents.values())
        assert list(descriptor.unpack(tcd, rec)) == [
            ('coefficients', [
                Coefficient(1.5, 42.0, constituents[0]),
                ]),
            ]

    def test_pack(self, descriptor, tcd, station):
        from libtcd.api import Coefficient

        constituents = list(tcd.constituents.values())
        station.coefficients = [
            Coefficient(1.5, 42.0, constituents[0]),
            ]
        result = dict(descriptor.pack(tcd, station))
        assert set(result.keys()) == set(['amplitude', 'epoch'])
        assert list(result['amplitude']) == [1.5] + [0] * 254
        assert list(result['epoch']) == [42.0] + [0] * 254

class Test_reference_station(attr_descriptor_test_base):
    @pytest.fixture
    def descriptor(self):
        from libtcd.api import _reference_station
        return _reference_station('reference_station')

    @pytest.fixture
    def tcd(self, test_tcd):
        return test_tcd

    @pytest.fixture
    def values(self, tcd):
        packed = 0
        unpacked = tcd[packed]
        return packed, unpacked

    def test_unpack(self, descriptor, tcd, rec, values):
        packed, unpacked = values
        result = dict(descriptor.unpack(tcd, rec))
        refstation, = result.values()
        assert refstation.name == unpacked.name
        assert refstation.record_number == unpacked.record_number

    def test_unpack_value(self, descriptor, tcd, values):
        packed, unpacked = values
        result = descriptor.unpack_value(tcd, packed)
        assert result.name == unpacked.name
        assert result.record_number == unpacked.record_number
