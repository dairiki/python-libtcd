# -*- coding: utf-8 -*-
"""
"""
from __future__ import absolute_import

from collections import namedtuple, Mapping
from ctypes import c_char_p, POINTER
import datetime
from itertools import chain, count, islice
from operator import attrgetter, methodcaller
from threading import Lock
import re

from six import text_type
from six.moves import range, zip

from . import _libtcd
from .compat import bytes_, OrderedDict
from .util import timedelta_total_minutes

Constituent = namedtuple('Constituent', ['name', 'speed', 'node_factors'])

NodeFactor = namedtuple('NodeFactor', ['equilibrium', 'node_factor'])

class NodeFactors(Mapping):
    """ Mapping from ``year`` to :cls:`NodeFactor`\s
    """
    def __init__(self, start_year, node_factors):
        self.start_year = start_year
        self.node_factors = node_factors

    @property
    def end_year(self):
        return self.start_year + len(self.node_factors)

    def __len__(self):
        return len(self.node_factors)

    def __iter__(self):
        return range(self.start_year, self.end_year)

    def values(self):
        # FIXME: py3k compatibility (need to return a view?)
        return self.node_factors

    def __getitem__(self, year):
        return self.node_factors[int(year) - self.start_year]

Coefficient = namedtuple('Coefficient', ['amplitude', 'epoch', 'constituent'])

class StationHeader(object):
    _attributes = [
        ('record_number', None),
        ('latitude', None),
        ('longitude', None),
        ('tzfile', u'Unknown'),
        ]

    @classmethod
    def _attrs(cls):
        return chain.from_iterable(
            c.__dict__.get('_attributes', ()) for c in cls.__mro__)

    def __init__(self, name, **kwargs):
        self.name = name
        for key, dflt in self._attrs():
            try:
                value = kwargs.pop(key)
            except KeyError:
                value = dflt() if callable(dflt) else dflt
            setattr(self, key, value)
        for key in kwargs:
            raise TypeError(
                "__init__() got an unexpected keyword argument %r", key)

    def __repr__(self):
        return "<{0.__class__.__name__}: {0.name}>".format(self)

class ReferenceStationHeader(StationHeader):
    pass

class SubordinateStationHeader(StationHeader):
    def __init__(self,
                 name,
                 reference_station,
                 **kwargs):
        super(SubordinateStationHeader, self).__init__(name, **kwargs)
        self.reference_station = reference_station

class Station(StationHeader):
    _attributes = [
        ('country', u'Unknown'),
        ('source', None),
        ('restriction', u'Non-commercial use only'),
        ('comments', None),
        ('notes', u''),
        ('legalese', None),
        ('station_id_context', None),
        ('station_id', None),
        ('date_imported', None),
        ('xfields', OrderedDict),
        ('direction_units', None),  # XXX: or should default be u'Unknown'?
        ('min_direction', None),
        ('max_direction', None),
        ('level_units', u'Unknown'),
        ]


class ReferenceStation(ReferenceStationHeader, Station):
    _attributes = [
        ('datum_offset', 0.0),
        ('datum', u'Unknown'),
        ('zone_offset', datetime.timedelta(0)),
        ('expiration_date', None),
        ('months_on_station', 0),
        ('last_date_on_station', None),
        ('confidence', 9),
        ]

    def __init__(self, name, coefficients, **kw):
        super(ReferenceStation, self).__init__(name, **kw)
        self.coefficients = coefficients

class SubordinateStation(SubordinateStationHeader, Station):
    _attributes = [
        ('min_time_add', None),     # XXX: or timedelta(0)?
        ('min_level_add', 0.0),
        ('min_level_multiply', None),
        ('max_time_add', None),
        ('max_level_add', 0.0),
        ('max_level_multiply', None),
        ('flood_begins', None),
        ('ebb_begins', None),
        ]

_lock = Lock()
_current_database = None

def get_current_database():
    return _current_database

class Tcd(object):

    def __init__(self, filename, constituents):
        global _current_database
        packed_constituents = self._pack_constituents(constituents)
        self.filename = filename
        bfilename = bytes_(filename, _libtcd.ENCODING)
        with _lock:
            _current_database = None
            rv = _libtcd.create_tide_db(bfilename, *packed_constituents)
            assert rv                   # FIXME: raise real exception
            _current_database = self
            self._init()

    @classmethod
    def open(cls, filename):
        self = cls.__new__(cls)
        self.filename = filename
        with self:
            self._init()
        return self

    def __enter__(self):
        global _current_database
        bfilename = bytes_(self.filename, _libtcd.ENCODING)
        _lock.acquire()
        try:
            if _current_database != self:
                rv = _libtcd.open_tide_db(bfilename)
                assert rv               # FIXME: raise real exception
                _current_database = self
            return self
        except:
            _lock.release()
            raise

    def __exit__(self, exc_typ, exc_val, exc_tb):
        _lock.release()

    def close(self):
        global _current_database
        with _lock:
            if _current_database == self:
                _libtcd.close_tide_db()
                _current_database = None

    def __len__(self):
        return self._header.number_of_records

    def __iter__(self):
        try:
            for i in count():
                yield self[i]
        except IndexError:
            pass

    def __getitem__(self, i):
        if i < 0:
            i += len(self)
        with self:
            rec = _libtcd.read_tide_record(i)
            if rec is None:
                raise IndexError(i)
            return _unpack_tide_record(self, rec)

    def __setitem__(self, i, station):
        with self:
            rec = _pack_tide_record(self, station)
            rv = _libtcd.update_tide_record(i, rec, self._header)
            assert rv                   # FIXME: raise real exception

    def __delitem__(self, i):
        with self:
            rv = _libtcd.delete_tide_record(i, self._header)
            assert rv                   # FIXME: raise real exception

    def append(self, station):
        with self:
            self._append(station)

    def _append(self, station):
        rec = _pack_tide_record(self, station)
        rv = _libtcd.add_tide_record(rec, self._header)
        assert rv                   # FIXME: raise real exception

    def find(self, name):
        with self:
            i = _libtcd.find_station(bytes_(name, _libtcd.ENCODING))
            if i < 0:
                raise KeyError(name)
            rec = _libtcd.read_tide_record(i)
            return _unpack_tide_record(self, rec)

    def findall(self, name):
        bname = bytes_(name, _libtcd.ENCODING)
        return [ _unpack_tide_record(self, rec)
                 for rec in self_find_recs(bname) ]

    def _find_recs(self, name):
        _libtcd.search_station(b"")     # reset search (I hope)
        while True:
            i = _libtcd.search_station(name)
            if i < 0:
                break
            rec = _libtcd.read_tide_record(i)
            if rec.name == name:
                yield rec

    def index(self, station):
        target = _pack_tide_record(self, station)
        for rec in self._find_recs(target.name):
            if self.records_match(rec, target):
                return rec.record_number
        raise ValueError("Station %r not found" % station.name)

    @staticmethod
    def records_match(s1, s2):
        # XXX: should make this more paranoid?
        def key(rec):
            return rec.record_type, rec.name
        return key(s1) == key(s2)

    def dump_tide_record(self, i):
        """ Dump tide record to stderr (Debugging only.)
        """
        with self:
            rec = _libtcd.read_tide_record(i)
            if rec is None:
                raise IndexError(i)
            _libtcd.dump_tide_record(rec)

    def _init(self):
        self._header = _libtcd.get_tide_db_header()
        self.constituents = self._read_constituents()

    def _pack_constituents(self, constituents):
        start_year = max(map(attrgetter('node_factors.start_year'),
                             constituents.values()))
        end_year = min(map(attrgetter('node_factors.end_year'),
                           constituents.values()))
        num_years = end_year - start_year
        if num_years < 1:
            raise ValueError("num_years is zero")

        n = len(constituents)
        names = (c_char_p * n)()
        speeds = (_libtcd.c_float64 * n)()
        equilibriums = (POINTER(_libtcd.c_float32) * n)()
        node_factors = (POINTER(_libtcd.c_float32) * n)()

        for i, c in enumerate(constituents.values()):
            names[i] = bytes_(c.name, _libtcd.ENCODING)
            speeds[i] = c.speed
            equilibriums[i] = eqs = (_libtcd.c_float32 * num_years)()
            node_factors[i] = nfs = (_libtcd.c_float32 * num_years)()
            for j in range(num_years):
                eqs[j], nfs[j] = c.node_factors[start_year + j]

        return (n, names, speeds,
                start_year, num_years, equilibriums, node_factors)

    def _read_constituents(self):
        start_year = self._header.start_year
        number_of_years = self._header.number_of_years
        constituents = OrderedDict()
        for i in range(self._header.constituents):
            name = text_type(_libtcd.get_constituent(i), _libtcd.ENCODING)
            if name in constituents:
                raise InvalidTcdFile("duplicate constituent name (%r)" % name)
            speed = _libtcd.get_speed(i)
            factors = islice(
                zip(_libtcd.get_equilibriums(i), _libtcd.get_node_factors(i)),
                number_of_years)
            factors = (NodeFactor(eq, nf)
                       for eq, nf in zip(_libtcd.get_equilibriums(i),
                                         _libtcd.get_node_factors(i)))
            factors = list(islice(factors, number_of_years))
            node_factors = NodeFactors(start_year, factors)
            constituents[name] = Constituent(name, speed, node_factors)
        return constituents

_marker = object()

class _attr_descriptor(object):
    def __init__(self, name, packed_name=None, null_value=_marker, **kwargs):
        if packed_name is None:
            packed_name = name
        self.name = name
        self.packed_name = packed_name
        self.null_value = null_value
        self.__dict__.update(**kwargs)

    def unpack(self, tcd, rec):
        packed = getattr(rec, self.packed_name)
        if self.null_value is not _marker and packed == self.null_value:
            value = None
        else:
            value = self.unpack_value(tcd, packed)
        yield self.name, value

    def pack(self, tcd, station):
        value = getattr(station, self.name)
        if self.null_value is not _marker and value is None:
            packed = self.null_value
        else:
            packed = self.pack_value(tcd, value)
        yield self.packed_name, packed

    def unpack_value(self, tcd, value):
        return value

    def pack_value(self, tcd, value):
        return value

class _string_table(_attr_descriptor):
    getter_tmpl = 'get_{table_name}'
    finder_tmpl = 'find_or_add_{table_name}'

    def __init__(self, *args, **kwargs):
        super(_string_table, self).__init__(*args, **kwargs)

        table_name = getattr(self, 'table_name', self.packed_name)
        self.getter = getattr(_libtcd, self.getter_tmpl.format(**locals()))
        self.finder = getattr(_libtcd, self.finder_tmpl.format(**locals()))

    def unpack_value(self, tcd, i):
        return text_type(self.getter(i), _libtcd.ENCODING)

    def pack_value(self, tcd, s):
        if s is None:
            return 0
        i = self.finder(bytes_(s, _libtcd.ENCODING))
        if i < 0:
            raise ValueError(s)         # FIXME: better message
        return i

class _string_enum(_string_table):
    finder_tmpl = 'find_{table_name}'

class _string(_attr_descriptor):
    def unpack_value(self, tcd, b):
        return text_type(b, _libtcd.ENCODING)

    def pack_value(self, tcd, s):
        return bytes_(s, _libtcd.ENCODING)

class _date(_attr_descriptor):
    @staticmethod
    def unpack_value(tcd, packed):
        yyyy, mmdd = divmod(int(packed), 10000)
        mm, dd = divmod(mmdd, 100)
        return datetime.date(yyyy, mm, dd)

    @staticmethod
    def pack_value(tcd, date):
        return date.year * 10000 + date.month * 100 + date.day

class _time_offset(_attr_descriptor):
    @staticmethod
    def unpack_value(tcd, packed):
        sign = 1 if packed >= 0 else -1
        hours, minutes = divmod(abs(packed), 100)
        assert 0 <= minutes < 60        # FIXME: ValueError instead?
        return sign * timeoffset(hours=hours, minutes=minutes)

    @staticmethod
    def pack_value(tcd, offset):
        if offset is None:
            return 0;
        minutes = timedelta_total_minutes(offset)
        sign = 1 if minutes > 0 else -1
        hh, mm = divmod(abs(minutes), 60)
        return sign * (100 * hh + mm)

# FIXME: move
class timeoffset(datetime.timedelta):
    ''' A :cls:`datetime.timedelta` which stringifies to "[-+]HH:MM"
    '''
    def __str__(self):
        minutes = timedelta_total_minutes(self)
        if minutes == 0:
            return '0:00'
        else:
            sign = '-' if minutes < 0.0 else '+'
            hh, mm = divmod(abs(minutes), 60)
            return "%s%02d:%02d" % (sign, hh, mm)

class _direction(_attr_descriptor):
    @staticmethod
    def unpack_value(tcd, packed):
        if not 0 <= packed < 360:
            return None                 # be lenient about ignore bad directions
        return packed

    @staticmethod
    def pack_value(tcd, direction):
        if not 0 <= direction < 360:
            raise ValueError(direction)
        return int(direction)

class _xfields(_attr_descriptor):
    @staticmethod
    def unpack_value(tcd, packed):
        s = text_type(packed, _libtcd.ENCODING)
        xfields = OrderedDict()
        for m in re.finditer(r'([^\n]+):([^\n]*(?:\n [^\n]*)*)', s):
            k, v = m.groups()
            xfields[k] = '\n'.join(v.split('\n '))
        return xfields

    @staticmethod
    def pack_value(tcd, xfields):
        pieces = []
        for k, v in xfields.items():
            pieces.extend([bytes_(k, _libtcd.ENCODING), b':'])
            lines = bytes_(v, _libtcd.ENCODING).split(b'\n')
            for line in lines[:-1]:
                pieces.extend([line, b'\n '])
            pieces.extend([lines[-1], b'\n'])
        return b''.join(pieces)

class _record_number(_attr_descriptor):
    def pack(self, tcd, station):
        return ()                       # never pack record number

class _record_type(_attr_descriptor):
    def unpack(self, tcd, rec):
        return ()

    def pack(self, tcd, station):
        if isinstance(station, ReferenceStation):
            record_type = _libtcd.REFERENCE_STATION
        else:
            assert isinstance(station, SubordinateStation)
            record_type = _libtcd.SUBORDINATE_STATION
        yield self.name, record_type

class _coordinates(_attr_descriptor):
    # latitude/longitude
    def unpack(self, tcd, rec):
        latitude = rec.latitude
        longitude = rec.longitude
        if latitude == 0 and longitude == 0:
            latitude = longitude = None
        yield 'latitude', latitude
        yield 'longitude', longitude

    def pack(self, tcd, station):
        latitude = station.latitude
        longitude = station.longitude
        # XXX: Warning if latitude == longitude == 0
        if latitude is None or longitude is None:
            latitude = longitude = 0
            # XXX: Warning if latitude is not None or longitude is not None?
        yield 'latitude', latitude
        yield 'longitude', longitude

class _coefficients(_attr_descriptor):
    # latitude/longitude
    def unpack(self, tcd, rec):
        yield self.name, [
            Coefficient(amplitude, epoch, constituent)
            for constituent, amplitude, epoch in zip(
                tcd.constituents.values(), rec.amplitude, rec.epoch)
            if amplitude != 0.0
            ]

    def pack(self, tcd, station):
        coeffs = dict((coeff.constituent.name, coeff)
                      for coeff in station.coefficients)
        coeff_t = _libtcd.c_float32 * 255
        amplitudes = coeff_t()
        epochs = coeff_t()
        for n, constituent in enumerate(tcd.constituents):
            coeff = coeffs.pop(constituent, None)
            if coeff is not None:
                amplitudes[n] = coeff.amplitude
                epochs[n] = coeff.epoch
        assert len(coeffs) == 0     # FIXME: better diagnostics
        yield 'amplitude', amplitudes
        yield 'epoch', epochs

class _reference_station(_attr_descriptor):
    @staticmethod
    def unpack_value(tcd, i):
        refrec = _libtcd.read_tide_record(i)
        assert refrec.record_type == _libtcd.REFERENCE_STATION
        return _unpack_tide_record(tcd, refrec)

    @staticmethod
    def pack_value(tcd, refstation):
        assert isinstance(refstation, ReferenceStation)
        try:
            i = tcd.index(refstation)
        except ValueError:
            tcd._append(refstation)
            i = len(tcd) - 1
        return i

_COMMON_ATTRS = [
    _record_number('record_number'),
    _record_type('record_type'),
    _string('name'),
    _coordinates('latitude/longitude'), # latitude and longitude
    _string('source', null_value=b''),
    _string('comments', null_value=b''),
    _string('notes'),
    _string('station_id_context', null_value=b''),
    _string('station_id', null_value=b''),
    _xfields('xfields'),

    _date('date_imported', null_value=0),

    _string_table('tzfile'),
    _string_table('country'),
    _string_table('restriction'),
    _string_table('legalese', null_value=0),

    # FIXME: make these real enums?
    _string_enum('level_units'),
    # FIXME: make None if not current?
    _string_enum('direction_units', table_name='dir_units'),
    _direction('min_direction', null_value=361),
    _direction('max_direction', null_value=361),
    ]

_REFSTATION_ATTRS = _COMMON_ATTRS + [
    _attr_descriptor('datum_offset'),
    _string_table('datum'),
    _time_offset('zone_offset'),
    _date('expiration_date', null_value=0),
    _attr_descriptor('months_on_station'),
    _date('last_date_on_station', null_value=0),
    _attr_descriptor('confidence'),
    _coefficients('coefficients'),
    ]

_SUBSTATION_ATTRS = _COMMON_ATTRS + [
    _reference_station('reference_station', null_value=-1),
    _time_offset('min_time_add'),
    _attr_descriptor('min_level_add'),
    _attr_descriptor('min_level_multiply', null_value=0.0),
    _time_offset('max_time_add'),
    _attr_descriptor('max_level_add'),
    _attr_descriptor('max_level_multiply', null_value=0.0),
    _time_offset('flood_begins', null_value=_libtcd.NULLSLACKOFFSET),
    _time_offset('ebb_begins', null_value=_libtcd.NULLSLACKOFFSET),
    ]

def _unpack_tide_record(tcd, rec):
    if rec.record_type == _libtcd.REFERENCE_STATION:
        attrs = _REFSTATION_ATTRS
        station_class = ReferenceStation
    else:
        assert rec.record_type == _libtcd.SUBORDINATE_STATION
        attrs = _SUBSTATION_ATTRS
        station_class = SubordinateStation

    unpack = methodcaller('unpack', tcd, rec)
    return station_class(**dict(chain.from_iterable(map(unpack, attrs))))

# "Null" values for TIDE_RECORD fields in those cases where the "null" value
# is not zero.
_TIDE_RECORD_DEFAULTS = {
    'reference_station': -1,
    'min_direction': 361,
    'max_direction': 361,
    'flood_begins': _libtcd.NULLSLACKOFFSET,
    'ebb_begins': _libtcd.NULLSLACKOFFSET,
    }

def _pack_tide_record(tcd, station):
    packed = _TIDE_RECORD_DEFAULTS.copy()
    if isinstance(station, ReferenceStation):
        attrs = _REFSTATION_ATTRS
    else:
        assert isinstance(station, SubordinateStation)
        attrs = _SUBSTATION_ATTRS
    pack = methodcaller('pack', tcd, station)
    packed.update(chain.from_iterable(map(pack, attrs)))
    return _libtcd.TIDE_RECORD(**packed)
