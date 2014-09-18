# -*- coding: utf-8 -*-
"""
"""
from __future__ import absolute_import

from collections import namedtuple, OrderedDict, Mapping, Sequence
from ctypes import c_char_p, POINTER
import datetime
from itertools import count, islice, izip
from operator import attrgetter
from threading import Lock

from six import binary_type, text_type

from . import _libtcd

_lock = Lock()
_current_database = None

def get_current_database():
    return _current_database

class _string_table(Sequence):
    def __init__(self, name, len_attr):
        self._name = name
        self._len_attr = len_attr

    def __len__(self):
        if _current_database:
            header = _current_database._header
        else:
            header = _libtcd.get_tide_db_header()
        return getattr(header, self._len_attr)

    def __getitem__(self, i):
        if not 0 <= int(i) < len(self):
            raise IndexError(i)
        getter = getattr(_libtcd, 'get_%s' % self._name)
        return text_type(getter(i), _libtcd.ENCODING)

    def index(self, value):
        finder = getattr(_libtcd, 'find_%s' % self._name)
        i = finder(bytes_(value))
        if i < 0:
            raise ValueError(value)
        return i

    def __contains__(self, value):
        try:
            self.index(value)
        except ValueError:
            return False
        return True

class _addable_string_table(_string_table):
    # XXX: not needed?
    def add(self, value):
        adder = getattr(_libtcd, 'add_%s' % self._name)
        header = _current_database._header if _current_database else None
        return adder(bytes_(value), header)

    def find_or_add(self, value):
        find_or_adder = getattr(_libtcd, 'find_or_add_%s' % self._name)
        header = _current_database._header if _current_database else None
        return find_or_adder(bytes_(value), header)

_restrictions = _addable_string_table('restriction', 'restriction_types')
_tzfiles = _addable_string_table('tzfile', 'tzfiles')
_countries = _addable_string_table('country', 'countries')
_datums = _addable_string_table('datum', 'datum_types')
_legaleses = _addable_string_table('legalese', 'legaleses')
_level_units = _string_table('level_units', 'level_unit_types')
_dir_units = _string_table('dir_units', 'dir_unit_types')

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
            return self._station(rec)

    def __setitem__(self, i, station):
        with self:
            rec = self._record(station)
            rv = _libtcd.update_tide_record(i, rec, self._header)
            assert rv                   # FIXME: raise real exception

    def __delitem__(self, i):
        with self:
            rv = _libtcd.delete_tide_record(i, self._header)
            assert rv                   # FIXME: raise real exception

    def append(self, station):
        with self:
            rec = self._record(station)
            rv = _libtcd.add_tide_record(rec, self._header)
            assert rv                   # FIXME: raise real exception

    def find(self, name):
        with self:
            i = _libtcd.find_station(bytes_(name))
            if i < 0:
                raise KeyError(name)
            rec = _libtcd.read_tide_record(i)
            return self._station(rec)

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
                    stations.append(self._station(rec))
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
            names[i] = c.name
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
                izip(_libtcd.get_equilibriums(i), _libtcd.get_node_factors(i)),
                number_of_years)
            factors = (NodeFactor(eq, nf)
                       for eq, nf in izip(_libtcd.get_equilibriums(i),
                                           _libtcd.get_node_factors(i)))
            factors = list(islice(factors, number_of_years))
            node_factors = NodeFactors(start_year, factors)
            constituents[name] = Constituent(name, speed, node_factors)
        return constituents

    def _station(self, rec):
        if rec.header.record_type == _libtcd.REFERENCE_STATION:
            return self._reference_station(rec)
        else:
            return self._subordinate_station(rec)

    def _common_attrs(self, rec):
        header = rec.header
        latitude = header.latitude
        longitude = header.longitude
        if latitude == 0.0 and longitude == 0.0:
            latitude = longitude = None
        return dict(
            record_number=header.record_number,
            latitude=latitude,
            longitude=longitude,
            tzfile=_tzfiles[header.tzfile],
            name=text_type(header.name, _libtcd.ENCODING),
            country=_countries[rec.country],
            source=text_(rec.source or None),
            restriction=_restrictions[rec.restriction],
            comments=text_(rec.comments or None),
            notes=text_type(rec.notes, _libtcd.ENCODING),
            legalese=(_legaleses[rec.legalese]
                      if rec.legalese != 0 else None),
            station_id_context=text_(rec.station_id_context or None),
            station_id=text_(rec.station_id or None),
            date_imported=unpack_date(rec.date_imported),
            xfields=text_type(rec.xfields, _libtcd.ENCODING),
            direction_units=_dir_units[rec.direction_units],
            min_direction=unpack_dir(rec.min_direction),
            max_direction=unpack_dir(rec.max_direction),
            level_units=_level_units[rec.level_units],
            )

    def _reference_station(self, rec):
        assert rec.header.record_type == _libtcd.REFERENCE_STATION
        assert rec.header.reference_station == -1

        return ReferenceStation(
            datum_offset=rec.datum_offset,
            datum=_datums[rec.datum],
            zone_offset=unpack_offset(rec.zone_offset),
            expiration_date=unpack_date(rec.expiration_date),
            months_on_station=rec.months_on_station or None,
            last_date_on_station=unpack_date(rec.last_date_on_station),
            confidence=rec.confidence,
            coefficients=[
                Coefficient(amplitude, epoch, constituent)
                for constituent, amplitude, epoch in zip(
                    self.constituents.values(), rec.amplitude, rec.epoch)
                if amplitude != 0.0],
            **self._common_attrs(rec))

    def _subordinate_station(self, rec):
        header = rec.header
        assert header.record_type == _libtcd.SUBORDINATE_STATION

        refstation = _libtcd.read_tide_record(header.reference_station)
        assert refstation.header.record_type == _libtcd.REFERENCE_STATION

        return SubordinateStation(
            reference_station=self._reference_station(refstation),
            min_time_add=unpack_offset(rec.min_time_add),
            min_level_add=rec.min_level_add,
            min_level_multiply=unpack_level_multiply(rec.min_level_multiply),
            max_time_add=unpack_offset(rec.max_time_add),
            max_level_add=rec.max_level_add,
            max_level_multiply=unpack_level_multiply(rec.max_level_multiply),
            flood_begins=unpack_offset(rec.flood_begins,
                                       _libtcd.NULLSLACKOFFSET),
            ebb_begins=unpack_offset(rec.ebb_begins, _libtcd.NULLSLACKOFFSET),
            **self._common_attrs(rec))

    def _record(self, station):
        latitude = station.latitude
        longitude = station.longitude
        if latitude is None or longitude is None:
            latitude = longitude = 0.0
        rec = _libtcd.TIDE_RECORD(
            header=_libtcd.TIDE_STATION_HEADER(
                latitude=latitude,
                longitude=longitude,
                tzfile=_tzfiles.find_or_add(station.tzfile),
                name=bytes_(station.name)),
            country=_countries.find_or_add(station.country or "Unknown"),
            source=bytes_(station.source or ""),
            restriction=_restrictions.find_or_add(
                station.restriction or ""),
            comments=bytes_(station.comments or ""),
            notes=bytes_(station.notes or ""),
            legalese=_legaleses.find_or_add(station.legalese or "NULL"),
            station_id_context=bytes_(station.station_id_context or ""),
            station_id=bytes_(station.station_id or ""),
            date_imported=pack_date(station.date_imported),
            xfields=bytes_(station.xfields or ""),
            direction_units=_dir_units.index(station.direction_units),
            min_direction=pack_dir(station.min_direction),
            max_direction=pack_dir(station.max_direction),
            level_units=_level_units.index(station.level_units),
            )

        if isinstance(station, ReferenceStation):
            rec.header.record_type = _libtcd.REFERENCE_STATION
            rec.header.reference_station = -1
            rec.datum_offset = station.datum_offset
            rec.datum = (_datums.find_or_add(station.datum)
                         if station.datum else 0)
            rec.zone_offset = pack_offset(station.zone_offset)
            rec.expiration_date = pack_date(station.expiration_date)
            rec.months_on_station = station.months_on_station or 0
            rec.last_date_on_station = pack_date(station.last_date_on_station)
            rec.confidence = station.confidence or 0

            coeffs = dict((coeff.constituent.name, coeff)
                          for coeff in station.coefficients)
            coeff_t = _libtcd.c_float32 * 255
            amplitudes = coeff_t()
            epochs = coeff_t()
            for n, constituent in enumerate(self.constituents):
                coeff = coeffs.pop(constituent, None)
                if coeff is not None:
                    amplitudes[n] = coeff.amplitude
                    epochs[n] = coeff.epoch
            assert len(coeffs) == 0     # FIXME: better diagnostics
            rec.amplitude = amplitudes
            rec.epoch = epochs

            flood_begins=_libtcd.NULLSLACKOFFSET,
            ebb_begins=_libtcd.NULLSLACKOFFSET,

        else:
            assert isinstance(station, SubordinateStation)
            rec.header.record_type = _libtcd.SUBORDINATE_STATION
            assert isinstance(station.reference_station, ReferenceStation)
            #FIXME:
            raise NotImplementedError()
            rec.header.reference_station = FIXME
            rec.min_time_add = pack_offset(station.min_time_add)
            rec.min_level_add = station.min_level_add or 0
            rec.min_level_multiply = pack_level_multiply(
                station.min_level_multiply)
            rec.max_time_add = pack_offset(station.max_time_add)
            rec.max_level_add = station.max_level_add or 0
            rec.max_level_multiply = pack_level_multiply(
                station.max_level_multiply)
            rec.flood_begins = pack_offset(station.flood_begins,
                                           _libtcd.NULLSLACKOFFSET)
            rec.ebb_begins = pack_offset(station.ebb_begins,
                                         _libtcd.NULLSLACKOFFSET)
        return rec

def unpack_date(packed):
    if packed != 0:
        yyyy, mmdd = divmod(int(packed), 10000)
        mm, dd = divmod(mmdd, 100)
        return datetime.date(yyyy, mm, dd)

def pack_date(date):
    if date is None:
        return 0
    return date.year * 10000 + date.month * 100 + date.day

def unpack_level_multiply(packed):
    if packed > 0.0:
        return packed

def pack_level_multiply(level_multiply):
    if level_multiply is None:
        return 0.0
    assert level_multiply > 0.0
    return level_multiply

def unpack_offset(packed, null_value=None):
    if packed != null_value:
        sign = 1 if packed >= 0 else -1
        hours, minutes = divmod(abs(packed), 100)
        assert 0 <= minutes < 60
        return sign * timeoffset(hours=hours, minutes=minutes)

def pack_offset(offset, null_value=0):
    if offset is None:
        return null_value
    sign = 1 if offset >= datetime.timedelta(0) else -1
    minutes = int(round(abs(offset.total_seconds()) / 60.0))
    hh, mm = divmod(minutes, 60)
    return sign * (100 * hh + mm)

def unpack_dir(packed):
    if 0 <= packed < 360:
        return packed

def pack_dir(direction):
    if direction is None:
        return 361
    assert 0 <= direction < 360
    return direction

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

def text_(s):
    if isinstance(s, binary_type):
        s = text_type(s, _libtcd.ENCODING)
    return s

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
