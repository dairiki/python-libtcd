# -*- coding: utf-8 -*-
"""
"""
from __future__ import absolute_import

from ctypes import c_float
import datetime
from functools import partial
from shutil import copyfileobj
import tempfile
from pkg_resources import resource_filename
import os

import pytest
from six import binary_type, integer_types

from libtcd.compat import OrderedDict
from libtcd.util import remove_if_exists

# FIXME: to move


class TestNodeFactors(object):
    def make_one(self, start_year, node_factors):
        from libtcd.api import NodeFactors
        return NodeFactors(start_year, node_factors)

    def make_node_factor(self, equilibrium, node_factor):
        from libtcd.api import NodeFactor
        return NodeFactor(equilibrium, node_factor)

    @pytest.fixture
    def node_factors(self):
        return [self.make_node_factor(1.0, 1.5)]

    @pytest.fixture
    def test_factors(self, node_factors):
        return self.make_one(1972, node_factors)

    def test_end_year(self, test_factors):
        assert test_factors.end_year == 1973

    def test_len(self, test_factors):
        assert len(test_factors) == 1

    def test_iter(self, test_factors):
        assert list(test_factors) == [1972]

    def test_values(self, test_factors, node_factors):
        assert list(test_factors.values()) == node_factors

    def test_getitem(self, test_factors, node_factors):
        assert test_factors[1972] == node_factors[0]
        with pytest.raises(KeyError):
            test_factors[1973]
        with pytest.raises(KeyError):
            test_factors[1971]


class TestStationHeader(object):
    def make_one(self, *args, **kwargs):
        from libtcd.api import StationHeader
        return StationHeader(*args, **kwargs)

    def test_defaults(self):
        header = self.make_one('Testing')
        assert header.tzfile == u'Unknown'

    @pytest.mark.parametrize('attr', ['badkw', '__init__'])
    def test_bad_kw(self, attr):
        kwargs = {attr: 'foo'}
        with pytest.raises(TypeError):
            self.make_one('Testing', **kwargs)

    def test_repr(self):
        header = self.make_one('Testing')
        assert repr(header) == "<StationHeader: Testing>"

################################################################
TCD_FILENAME = resource_filename('libtcd.tests', 'test.tcd')


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
    from libtcd.api import Tcd
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


@pytest.fixture
def uninitialized_tcd():
    from libtcd.api import Tcd
    from libtcd._libtcd import DB_HEADER_PUBLIC
    tcd = Tcd.__new__(Tcd)
    tcd._header = DB_HEADER_PUBLIC()
    return tcd


def check_not_locked():
    from libtcd.api import _lock
    assert _lock.acquire(False)
    _lock.release()


def test_get_current_database(test_tcd):
    from libtcd.api import get_current_database
    with test_tcd:
        assert get_current_database() == test_tcd


class TestApi(object):
    def test_enter_failure(self, temp_tcd):
        from libtcd import _libtcd
        os.unlink(temp_tcd.filename)
        temp_tcd.close()
        with pytest.raises(_libtcd.Error):
            temp_tcd.__enter__()
        check_not_locked()

    def test_constituents(self, new_tcd, dummy_constituents):
        constituents = new_tcd.constituents
        assert len(constituents) == 1
        assert list(constituents)[0] == 'J1'
        assert constituents['J1'].speed == dummy_constituents['J1'].speed

    def test_len(self, test_tcd):
        assert len(test_tcd) == 2

    def test_unpack_record_raises_invalid_tcd_file(self, uninitialized_tcd):
        from ..api import InvalidTcdFile
        from .. import _libtcd

        rec = _libtcd.TIDE_RECORD(record_type=3)
        with pytest.raises(InvalidTcdFile):
            uninitialized_tcd._unpack_record(rec)

    def test_headers_unpack_record_raises_invalid_tcd_file(
            self, uninitialized_tcd):
        from ..api import InvalidTcdFile
        from .. import _libtcd

        headers = uninitialized_tcd.headers
        rec = _libtcd.TIDE_STATION_HEADER(record_type=3)
        with pytest.raises(InvalidTcdFile):
            headers._unpack_record(rec)

    def test_getitem(self, test_tcd):
        assert test_tcd[0].name == u"Seattle, Puget Sound, Washington"
        with pytest.raises(IndexError):
            test_tcd[100000]
        with pytest.raises(IndexError):
            test_tcd[len(test_tcd)]
        assert test_tcd[-1].record_number == len(test_tcd) - 1

    def test_headers_getitem(self, test_tcd):
        headers = test_tcd.headers
        assert headers[0].name == u"Seattle, Puget Sound, Washington"
        with pytest.raises(IndexError):
            headers[100000]
        with pytest.raises(IndexError):
            headers[len(test_tcd)]
        assert headers[-1].record_number == len(test_tcd) - 1

    def test_setitem(self, temp_tcd, dummy_refstation):
        temp_tcd[1] = dummy_refstation
        assert temp_tcd[1].name == dummy_refstation.name

    def test_delitem(self, temp_tcd):
        ilen = len(temp_tcd)
        del temp_tcd[1]
        assert len(temp_tcd) == ilen - 1

    def test_delitem_deleting_refstation_deletes_subordinates(self, temp_tcd):
        del temp_tcd[0]
        assert len(temp_tcd) == 0

    def test_append_refstation(self, new_tcd, dummy_refstation):
        tcd = new_tcd
        tcd.append(dummy_refstation)
        assert len(tcd) == 1

    def test_append_substation(self, new_tcd, dummy_substation):
        tcd = new_tcd
        tcd.append(dummy_substation)
        assert len(tcd) == 2

    def test_iter(self, test_tcd):
        stations = list(test_tcd)
        assert [s.name for s in stations] == [
            "Seattle, Puget Sound, Washington",
            "Tacoma Narrows Bridge, Puget Sound, Washington",
            ]
        assert len(stations[0].coefficients) == 32

    def test_find(self, test_tcd):
        s = test_tcd.find("Seattle, Puget Sound, Washington")
        assert s.record_number == 0

    def test_find_raises_key_error(self, test_tcd):
        with pytest.raises(KeyError):
            test_tcd.find("Snaploops, Puget Sound, Washington")

    def test_findall(self, new_tcd, dummy_refstation):
        new_tcd.append(dummy_refstation)
        new_tcd.append(dummy_refstation)
        stations = new_tcd.findall(dummy_refstation.name)
        assert [s.record_number for s in stations] == [0, 1]

    def test_dump_tide_record(self, test_tcd, capfd):
        test_tcd.dump_tide_record(0)
        out, err = capfd.readouterr()
        assert out == ''
        assert "Seattle, Puget Sound, Washington" in err

    def test_index(self, new_tcd, dummy_refstation, dummy_substation):
        new_tcd.append(dummy_refstation)
        new_tcd.append(dummy_substation)
        assert new_tcd.index(dummy_refstation) == 0
        assert new_tcd.index(dummy_substation) == 1

    @pytest.mark.parametrize('i', [2, -1])
    def test_dump_tide_record_raises_index_error(self, test_tcd, i):
        with pytest.raises(IndexError):
            test_tcd.dump_tide_record(i)

    def test_pack_constituents(self, uninitialized_tcd, dummy_constituents):
        n, names, speeds, start_year, num_years, equilibriums, node_factors \
            = uninitialized_tcd._pack_constituents(dummy_constituents)
        assert n == 1
        assert list(names) == [b'J1']
        assert list(speeds) == [15.5854433]
        assert start_year == 1970
        assert num_years == 1
        assert len(equilibriums) == 1
        assert equilibriums[0][0] == 1.0
        assert len(node_factors) == 1
        assert node_factors[0][0] == 2.0

    def test_pack_constituents_raises_value_error(self, uninitialized_tcd):
        from libtcd.api import Constituent, NodeFactors, NodeFactor
        c1 = Constituent('Foo1', 1.234,
                         NodeFactors(1970, [NodeFactor(1.0, 2.0)]))
        c2 = Constituent('Bar2', 2.345,
                         NodeFactors(1971, [NodeFactor(1.0, 2.0)]))
        constituents = {c1.name: c1, c2.name: c2}
        with pytest.raises(ValueError):
            uninitialized_tcd._pack_constituents(constituents)

    @pytest.fixture
    def patch_constituents(self, monkeypatch):
        from libtcd import _libtcd
        patch_libtcd = partial(monkeypatch.setattr, _libtcd)
        patch_libtcd('get_constituent', lambda i: b'Foo1')
        patch_libtcd('get_speed', lambda i: 1.234)
        patch_libtcd('get_equilibriums', lambda i: [1.0])
        patch_libtcd('get_node_factors', lambda i: [2.0])

    def test_read_constituents(self, uninitialized_tcd, patch_constituents):
        tcd = uninitialized_tcd
        tcd._header.start_year = 1970
        tcd._header.number_of_years = 1
        tcd._header.constituents = 1
        tcd._read_constituents()

    def test_read_constituents_raises_invalid_tcd_file(
            self, uninitialized_tcd, patch_constituents):
        from libtcd.api import InvalidTcdFile
        tcd = uninitialized_tcd
        tcd._header.start_year = 1970
        tcd._header.number_of_years = 1
        tcd._header.constituents = 2
        with pytest.raises(InvalidTcdFile):
            tcd._read_constituents()



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

        monkeypatch.setattr(_libtcd, 'get_tzfile', get)
        monkeypatch.setattr(_libtcd, 'find_tzfile', find)
        monkeypatch.setattr(_libtcd, 'find_or_add_tzfile', find_or_add)
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
    # def test_pack_value_none(self, descriptor, tcd):
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
        from libtcd.api import InvalidTcdFile
        with pytest.raises(InvalidTcdFile):
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
    # def test_pack_value_none(self, descriptor, tcd):
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

    def test_pack_raises_type_error(self, descriptor, tcd):
        with pytest.raises(TypeError):
            list(descriptor.pack(tcd, None))


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

    def test_pack_raises_error_if_missing_constuent(
            self, descriptor, tcd, station):
        from libtcd.api import (
            Coefficient,
            Constituent,
            NodeFactors,
            NodeFactor)
        constituent = Constituent('Missing1', 1.234,
                                  NodeFactors(1970, [NodeFactor(1.0, 2.0)]))
        station.coefficients = [
            Coefficient(1.5, 42.0, constituent),
            ]
        with pytest.raises(ValueError):
            dict(descriptor.pack(tcd, station))


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

    def test_unpack_raises_invalid_tcd_file(self,
                                            descriptor, tcd, rec,
                                            monkeypatch):
        from libtcd import _libtcd
        from libtcd.api import InvalidTcdFile

        def read_tide_record(i):
            rec = _libtcd.TIDE_RECORD()
            rec.record_type = _libtcd.SUBORDINATE_STATION
            return rec
        monkeypatch.setattr(_libtcd, 'read_tide_record', read_tide_record)
        with pytest.raises(InvalidTcdFile):
            for name, packed in descriptor.unpack(tcd, rec):
                pass            # pragma: NO COVER

    def test_unpack_value(self, descriptor, tcd, rec):
        with pytest.raises(NotImplementedError):
            descriptor.unpack_value(tcd, rec)

    def test_pack_value_raises_type_error(
            self, descriptor, uninitialized_tcd, dummy_substation):
        with pytest.raises(TypeError):
            descriptor.pack_value(uninitialized_tcd, dummy_substation)
