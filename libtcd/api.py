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

from six import add_metaclass, text_type
from six.moves import range, zip

from . import _libtcd
from .compat import bytes_, OrderedDict
from .util import reify, timedelta_total_minutes

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
        return iter(range(self.start_year, self.end_year))

    def values(self):
        # FIXME: py3k compatibility (need to return a view?)
        return self.node_factors

    def __getitem__(self, year):
        i = int(year) - self.start_year
        if 0 <= i < len(self.node_factors):
            return self.node_factors[i]
        raise KeyError(year)

Coefficient = namedtuple('Coefficient', ['amplitude', 'epoch', 'constituent'])


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
        if minutes < 0 or minutes >= 60:
            raise InvalidTcdFile(
                "Minutes out of range in time offset %r" % packed)
        return sign * timeoffset(hours=hours, minutes=minutes)

    @staticmethod
    def pack_value(tcd, offset):
        if offset is None:
            return 0
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
            return None       # be lenient: ignore bad directions
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
        elif isinstance(station, SubordinateStation):
            record_type = _libtcd.SUBORDINATE_STATION
        else:
            raise TypeError(
                "%r is neither a ReferenceStation nor SubordinateStation"
                % station)
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
        if coeffs:
            raise ValueError(
                "Tcd file is missing constituent(s): %s" % ' '.join(coeffs))
        yield 'amplitude', amplitudes
        yield 'epoch', epochs


class _reference_station(_attr_descriptor):
    def unpack(self, tcd, rec):
        if isinstance(rec, _libtcd.TIDE_STATION_HEADER):
            get_record = _libtcd.get_partial_tide_record
            refclass = ReferenceStationHeader
        else:
            assert isinstance(rec, _libtcd.TIDE_RECORD)
            get_record = _libtcd.read_tide_record
            refclass = ReferenceStation

        i = getattr(rec, self.packed_name)
        refrec = get_record(i)
        if refrec.record_type != _libtcd.REFERENCE_STATION:
            raise InvalidTcdFile("Reference station has bad record_type")
        yield self.name, refclass._unpack(tcd, refrec)

    @staticmethod
    def unpack_value(tcd, i):
        raise NotImplementedError("unused")

    @staticmethod
    def pack_value(tcd, refstation):
        if not isinstance(refstation, ReferenceStation):
            raise TypeError("%r is not a ReferenceStation" % refstation)
        try:
            i = tcd.index(refstation)
        except ValueError:
            i = tcd.append(refstation)
        return i


class _StationMeta(type):
    def __init__(cls, name, bases, dct):
        packed_attrs = set()
        for base in cls.__mro__:
            packed_attrs.update(getattr(base, '_PACKED_ATTRS', ()))
        if packed_attrs:
            cls._PACKED_ATTRS = tuple(packed_attrs)


@add_metaclass(_StationMeta)
class StationHeader(object):
    record_number = None
    latitude = None
    longitude = None
    tzfile = u'Unknown'

    def __init__(self, name, **kwargs):
        self.name = name
        for attr in kwargs:
            if attr.startswith('_') or not hasattr(self, attr):
                raise TypeError(
                    "__init__() got an unexpected keyword argument %r", attr)
        self.__dict__.update(kwargs)

    @classmethod
    def _unpack(cls, tcd, rec):
        unpack = methodcaller('unpack', tcd, rec)
        inst = cls.__new__(cls)
        inst.__dict__.update(
            chain.from_iterable(map(unpack, cls._PACKED_ATTRS)))
        return inst

    def __repr__(self):
        return "<{0.__class__.__name__}: {0.name}>".format(self)

    _PACKED_ATTRS = (
        _record_number('record_number'),
        _record_type('record_type'),
        _string('name'),
        _coordinates('latitude/longitude'),
        _string_table('tzfile'),
        )


class ReferenceStationHeader(StationHeader):
    pass


class SubordinateStationHeader(StationHeader):
    def __init__(self, name, reference_station, **kwargs):
        super(SubordinateStationHeader, self).__init__(name, **kwargs)
        self.reference_station = reference_station

    _PACKED_ATTRS = (
        _reference_station('reference_station', null_value=-1),
        )


class Station(StationHeader):
    country = u'Unknown'
    source = None
    restriction = u'Non-commercial use only'
    comments = None
    notes = u''
    legalese = None
    station_id_context = None
    station_id = None
    date_imported = None
    direction_units = None  # XXX: or should default be u'Unknown'?
    min_direction = None
    max_direction = None
    level_units = u'Unknown'

    @reify
    def xfields(self):
        return OrderedDict()

    def _pack(self, tcd):
        packed = self._TIDE_RECORD_DEFAULTS.copy()
        pack = methodcaller('pack', tcd, self)
        packed.update(
            chain.from_iterable(map(pack, self._PACKED_ATTRS)))
        return _libtcd.TIDE_RECORD(**packed)

    # "Null" values for TIDE_RECORD fields in those cases where the
    # "null" value is not zero.
    _TIDE_RECORD_DEFAULTS = {
        'reference_station': -1,
        'min_direction': 361,
        'max_direction': 361,
        'flood_begins': _libtcd.NULLSLACKOFFSET,
        'ebb_begins': _libtcd.NULLSLACKOFFSET,
        }

    _PACKED_ATTRS = (
        _string('source', null_value=b''),
        _string('comments', null_value=b''),
        _string('notes'),
        _string('station_id_context', null_value=b''),
        _string('station_id', null_value=b''),
        _xfields('xfields'),

        _date('date_imported', null_value=0),

        _string_table('country'),
        _string_table('restriction'),
        _string_table('legalese', null_value=0),

        # FIXME: make these real enums?
        _string_enum('level_units'),
        # FIXME: make None if not current?
        _string_enum('direction_units', table_name='dir_units'),
        _direction('min_direction', null_value=361),
        _direction('max_direction', null_value=361),
        )


class ReferenceStation(ReferenceStationHeader, Station):
    datum_offset = 0.0
    datum = u'Unknown'
    zone_offset = datetime.timedelta(0)
    expiration_date = None
    months_on_station = 0
    last_date_on_station = None
    confidence = 9

    def __init__(self, name, coefficients, **kw):
        super(ReferenceStation, self).__init__(name, **kw)
        self.coefficients = coefficients

    _PACKED_ATTRS = (
        _attr_descriptor('datum_offset'),
        _string_table('datum'),
        _time_offset('zone_offset'),
        _date('expiration_date', null_value=0),
        _attr_descriptor('months_on_station'),
        _date('last_date_on_station', null_value=0),
        _attr_descriptor('confidence'),
        _coefficients('coefficients'),
        )


class SubordinateStation(SubordinateStationHeader, Station):
    min_time_add = None         # XXX: or timedelta(0)?
    min_level_add = 0.0
    min_level_multiply = None
    max_time_add = None
    max_level_add = 0.0
    max_level_multiply = None
    flood_begins = None
    ebb_begins = None

    _PACKED_ATTRS = (
        _time_offset('min_time_add'),
        _attr_descriptor('min_level_add'),
        _attr_descriptor('min_level_multiply', null_value=0.0),
        _time_offset('max_time_add'),
        _attr_descriptor('max_level_add'),
        _attr_descriptor('max_level_multiply', null_value=0.0),
        _time_offset('flood_begins', null_value=_libtcd.NULLSLACKOFFSET),
        _time_offset('ebb_begins', null_value=_libtcd.NULLSLACKOFFSET),
        )


class InvalidTcdFile(Exception):
    """ Exception raised when a corrupt TCD file is encountered.
    """

_lock = Lock()
_current_database = None


def get_current_database():
    return _current_database


class _SequenceMixin(object):
    def __len__(self):          # pragma: NO COVER
        raise NotImplementedError()

    def _get_record(self, i):   # pragma: NO COVER
        raise NotImplementedError()

    def _unpack_record(self, rec):  # pragma: NO COVER
        raise NotImplementedError()

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
            rec = self._get_record(i)
        if rec is None:
            raise IndexError(i)
        return self._unpack_record(rec)

    def find(self, name):
        bname = bytes_(name, _libtcd.ENCODING)
        with self:
            i = _libtcd.find_station(bname)
            if i < 0:
                raise KeyError(name)
            rec = self._get_record(i)
        return self._unpack_record(rec)

    def findall(self, name):
        bname = bytes_(name, _libtcd.ENCODING)
        with self:
            records = self._find_recs(bname)
        return map(self._unpack_record, records)

    def _find_recs(self, name):
        _libtcd.search_station(b"")     # reset search (I hope)
        matches = []
        while True:
            i = _libtcd.search_station(name)
            if i < 0:
                break
            rec = self._get_record(i)
            if rec.name == name:
                matches.append(rec)
        return matches

    def index(self, station):
        if hasattr(station, 'reference_station'):
            record_type = _libtcd.SUBORDINATE_STATION
        else:
            record_type = _libtcd.REFERENCE_STATION
        bname = station.name.encode(_libtcd.ENCODING)

        with self:
            records = self._find_recs(bname)
        for rec in records:
            if (rec.record_type, rec.name) == (record_type, bname):
                return rec.record_number
        raise ValueError("Station %r not found" % station.name)


class Tcd(_SequenceMixin):

    def __init__(self, filename, constituents):
        global _current_database
        packed_constituents = self._pack_constituents(constituents)
        self.filename = filename
        bfilename = bytes_(filename, _libtcd.ENCODING)
        with _lock:
            _current_database = None
            _libtcd.create_tide_db(bfilename, *packed_constituents)
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
                _libtcd.open_tide_db(bfilename)
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

    @property
    def headers(self):
        return TcdHeaders(self)

    def __len__(self):
        return self._header.number_of_records

    def _get_record(self, i):
        return _libtcd.read_tide_record(i)

    def _unpack_record(self, rec):
        record_type = rec.record_type
        if record_type == _libtcd.REFERENCE_STATION:
            station_class = ReferenceStation
        elif record_type == _libtcd.SUBORDINATE_STATION:
            station_class = SubordinateStation
        else:
            raise InvalidTcdFile("Invalid record_type (%r)" % record_type)
        return station_class._unpack(self, rec)

    def __setitem__(self, i, station):
        rec = station._pack(self)
        with self:
            _libtcd.update_tide_record(i, rec, self._header)

    def __delitem__(self, i):
        with self:
            _libtcd.delete_tide_record(i, self._header)

    def append(self, station):
        """ Append station to database.

        Returns the index of the appended station.

        """
        rec = station._pack(self)
        with self:
            _libtcd.add_tide_record(rec, self._header)
            return self._header.number_of_records - 1

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

    @staticmethod
    def _pack_constituents(constituents):
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
            factors = (NodeFactor(eq, nf)
                       for eq, nf in zip(_libtcd.get_equilibriums(i),
                                         _libtcd.get_node_factors(i)))
            factors = list(islice(factors, number_of_years))
            node_factors = NodeFactors(start_year, factors)
            constituents[name] = Constituent(name, speed, node_factors)
        return constituents


class TcdHeaders(_SequenceMixin):
    def __init__(self, tcd):
        self.tcd = tcd
        self.__enter__ = tcd.__enter__
        self.__exit__ = tcd.__exit__

    def __enter__(self):
        return self.tcd.__enter__()

    def __exit__(self, exc_typ, exc_val, exc_tb):
        self.tcd.__exit__(exc_typ, exc_val, exc_tb)

    def __len__(self):
        return len(self.tcd)

    def _get_record(self, i):
        return _libtcd.get_partial_tide_record(i)

    def _unpack_record(self, rec):
        record_type = rec.record_type
        if record_type == _libtcd.REFERENCE_STATION:
            station_class = ReferenceStationHeader
        elif record_type == _libtcd.SUBORDINATE_STATION:
            station_class = SubordinateStationHeader
        else:
            raise InvalidTcdFile("Invalid record_type (%r)" % record_type)
        return station_class._unpack(self.tcd, rec)
