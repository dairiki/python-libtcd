# -*- coding: utf-8 -*-
"""
"""
from __future__ import absolute_import

from collections import namedtuple, OrderedDict, Mapping
from ctypes import c_char_p, POINTER
import datetime
from itertools import chain, count, islice
from operator import attrgetter, methodcaller
from threading import Lock

from six import text_type
from six.moves import zip

from . import _libtcd

_lock = Lock()
_current_database = None

def get_current_database():
    return _current_database


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
        return xrange(self.start_year, self.end_year)

    def values(self):
        # FIXME: py3k compatibility (need to return a view?)
        return self.node_factors

    def __getitem__(self, year):
        return self.node_factors[int(year) - self.start_year]

class Tcd(object):

    def __init__(self, filename, constituents):
        global _current_database
        packed_constituents = self._pack_constituents(constituents)
        self.filename = filename
        with _lock:
            _current_database = None
            rv = _libtcd.create_tide_db(bytes_(filename), *packed_constituents)
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
        _lock.acquire()
        try:
            if _current_database != self:
                rv = _libtcd.open_tide_db(bytes_(self.filename))
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
            rec = _pack_tide_record(self, station)
            rv = _libtcd.add_tide_record(rec, self._header)
            assert rv                   # FIXME: raise real exception

    def find(self, name):
        with self:
            i = _libtcd.find_station(bytes_(name))
            if i < 0:
                raise KeyError(name)
            rec = _libtcd.read_tide_record(i)
            return _unpack_tide_record(self, rec)

    def findall(self, name):
        bname = bytes_(name)
        stations = []
        with self:
            _libtcd.search_station(b"")     # reset search (I hope)
            while True:
                i = _libtcd.search_station(bname)
                if i < 0:
                    break
                rec = _libtcd.read_tide_record(i)
                if rec.name == bname:
                    stations.append(_unpack_tide_record(self, rec))
        return stations

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
            names[i] = bytes_(c.name)
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
        i = self.finder(bytes_(s))
        if i < 0:
            raise ValueError(s)         # FIXME: better message
        return i

class _string_enum(_string_table):
    finder_tmpl = 'find_{table_name}'

class _string(_attr_descriptor):
    def unpack_value(self, tcd, b):
        return text_type(b, _libtcd.ENCODING)

    def pack_value(self, tcd, s):
        return bytes_(s)

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
        sign = 1 if offset >= datetime.timedelta(0) else -1
        minutes = int(round(abs(offset.total_seconds()) / 60.0))
        hh, mm = divmod(minutes, 60)
        # FIXME: error/warn if seconds != 0?
        return sign * (100 * hh + mm)

# FIXME: move
class timeoffset(datetime.timedelta):
    def __unicode__(self):
        minutes, seconds = divmod(int(self.total_seconds()), 60)
        # FIXME: issue warning instead of AssertionError
        assert seconds == 0 and self.microseconds == 0
        sign = '+'
        if minutes == 0:
            return '0:00'
        else:
            sign = '-' if minutes < 0.0 else '+'
            hh, mm = divmod(minutes, 60)
            return "%s%02d:%02d" % (sign, hh, mm)

    def __str__(self):
        return unicode(self).encode('ascii')

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

class _record_number(_attr_descriptor):
    def pack(self, tcd, station):
        return ()                       # never pack record number

class _coordinates(object):
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
        raise NotImplementedError()

_COMMON_ATTRS = [
    _record_number('record_number'),
    _string('name'),
    _coordinates(),                     # latitude and longitude
    _string('source', null_value=''),
    _string('comments', null_value=''),
    _string('notes'),
    _string('station_id_context', null_value=''),
    _string('station_id', null_value=''),
    _string('xfields'),

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

def _pack_tide_record(tcd, station):
    if isinstance(station, ReferenceStation):
        attrs = _REFSTATION_ATTRS
        # FIXME: convert these to attribute descriptors
        extra_attrs = dict(
            record_type=_libtcd.REFERENCE_STATION,
            reference_station=-1,
            flood_begins=_libtcd.NULLSLACKOFFSET,
            ebb_begins=_libtcd.NULLSLACKOFFSET,
            )
    else:
        assert isinstance(station, SubordinateStation)
        attrs = _SUBSTATION_ATTRS
        # FIXME: convert these to attribute descriptors
        extra_attrs = dict(
            record_type=_libtcd.SUBORDINATE_STATION,
            )

    pack = methodcaller('pack', tcd, station)
    packed = dict(chain.from_iterable(map(pack, attrs)))
    packed.update(extra_attrs)
    return _libtcd.TIDE_RECORD(**packed)

Coefficient = namedtuple('Coefficient', ['amplitude', 'epoch', 'constituent'])

class TcdRecord(object):
    def __init__(self,
                 record_number,
                 latitude, longitude,
                 tzfile, name,
                 country, source, restriction, comments, notes, legalese,
                 station_id_context, station_id,
                 date_imported,
                 xfields,
                 direction_units,
                 min_direction,
                 max_direction,
                 level_units):
        self.record_number = record_number
        self.latitude = latitude
        self.longitude = longitude
        self.tzfile = tzfile
        self.name = name
        self.country = country
        self.source = source
        self.restriction = restriction
        self.comments = comments
        self.notes = notes
        self.legalese = legalese
        self.station_id_context = station_id_context
        self.station_id = station_id
        self.date_imported = date_imported
        self.xfields = xfields
        self.direction_units = direction_units
        self.min_direction = min_direction
        self.max_direction = max_direction
        self.level_units = level_units

    def __repr__(self):
        return "<{0.__class__.__name__}: {0.name}>".format(self)

class ReferenceStation(TcdRecord):
    def __init__(self,
                 datum_offset,
                 datum,
                 zone_offset,
                 expiration_date,
                 months_on_station,
                 last_date_on_station,
                 confidence,
                 coefficients,
                 **kw):
        super(ReferenceStation, self).__init__(**kw)
        self.datum_offset = datum_offset
        self.datum = datum
        self.zone_offset = zone_offset
        self.expiration_date = expiration_date
        self.months_on_station = months_on_station
        self.last_date_on_station = last_date_on_station
        self.confidence = confidence
        self.coefficients = coefficients

class SubordinateStation(TcdRecord):
    def __init__(self,
                 reference_station,
                 min_time_add, min_level_add, min_level_multiply,
                 max_time_add, max_level_add, max_level_multiply,
                 flood_begins, ebb_begins,
                 **kw):
        super(SubordinateStation, self).__init__(**kw)
        self.reference_station = reference_station
        self.min_time_add = min_time_add
        self.min_level_add = min_level_add
        self.min_level_multiply = min_level_multiply
        self.max_time_add = max_time_add
        self.max_level_add = max_level_add
        self.max_level_multiply = max_level_multiply
        self.flood_begins = flood_begins
        self.ebb_begins = ebb_begins

def bytes_(s):
    if isinstance(s, text_type):
        s = s.encode(_libtcd.ENCODING)
    return s
