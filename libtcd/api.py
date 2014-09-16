# -*- coding: utf-8 -*-
"""
"""
from __future__ import absolute_import

import datetime
from operator import attrgetter
from threading import Lock

from six import text_type

from . import _libtcd
from ._libtcd import c_char_p, c_float32, c_float64, cast, pointer, POINTER

class Tcd(object):

    lock = Lock()
    _current_database = None

    def __init__(self, filename, constituents):
        start_year = max(map(attrgetter('start_year'), constituents))
        year_end = min(map(attrgetter('year_end'), constituents))
        num_years = year_end - start_year
        if num_years < 1:
            raise ValueError("num_years is zero")
        factor_t = c_float32 * num_years
        factors_t = POINTER(c_float32) * len(constituents)
        equilibriums = factors_t(*(
            factor_t(*(c.equilibriums[start_year - c.start_year:][:num_years]))
            for c in constituents))
        node_factors = factors_t(*(
            factor_t(*(c.node_factors[start_year - c.start_year:][:num_years]))
            for c in constituents))

        self.filename = filename

        with self.lock:
            type(self)._current_database = None
            rv = _libtcd.create_tide_db(
                bytes_(filename),
                len(constituents),
                (c_char_p * len(constituents))(
                    *map(attrgetter('name'), constituents)),
                (c_float64 * len(constituents))(
                    *map(attrgetter('speed'), constituents)),
                start_year, num_years,
                equilibriums, node_factors)
            assert rv               # FIXME: raise real exception
            type(self)._current_database = self
            self._init()

    @classmethod
    def open(cls, filename):
        self = cls.__new__(cls)
        self.filename = filename
        with self:
            self._init()
        return self

    def _init(self):
        self._header = _libtcd.get_tide_db_header()
        self.constituents = map(self._read_constituent,
                                range(self._header.constituents))

    @property
    def current_database(self):
        return type(self)._current_database

    def __enter__(self):
        self.lock.acquire()
        try:
            if self.current_database != self:
                rv = _libtcd.open_tide_db(bytes_(self.filename))
                assert rv               # FIXME: raise real exception
                type(self)._current_database = self
            return self
        except:
            self.lock.release()
            raise

    def __exit__(self, exc_typ, exc_val, exc_tb):
        self.lock.release()

    def close(self):
        with self.lock:
            if self.current_database == self:
                _libtcd.close_tide_db()
                type(self)._current_database = None

    def __len__(self):
        return self._header.number_of_records

    def __getitem__(self, i):
        with self:
            rec = _libtcd.read_tide_record(i)
            _libtcd.dump_tide_record(rec)
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

    def __iter__(self):
        for i in xrange(len(self)):
            yield self[i]

    def _read_constituent(self, num):
        header = self._header
        number_of_years = header.number_of_years
        return Constituent(
            header.start_year,
            name=_libtcd.get_constituent(num),
            speed=_libtcd.get_speed(num),
            equilibriums=_libtcd.get_equilibriums(num)[:number_of_years],
            node_factors=_libtcd.get_node_factors(num)[:number_of_years])

    def _station(self, rec):
        if rec.header.record_type == _libtcd.REFERENCE_STATION:
            return self._reference_station(rec)
        else:
            return self._subordinate_station(rec)

    def _common_attrs(self, rec):
        header = rec.header
        return dict(
            record_number=header.record_number,
            latitude=header.latitude,
            longitude=header.longitude,
            tzfile=_libtcd.get_tzfile(header.tzfile),
            name=header.name,
            country=_libtcd.get_country(rec.country),
            source=rec.source or None,
            restriction=_libtcd.get_restriction(rec.restriction),
            comments=rec.comments or None,
            notes=rec.notes,
            legalese=(_libtcd.get_legalese(rec.legalese)
                      if rec.legalese != 0 else None),
            station_id_context=rec.station_id_context or None,
            station_id=rec.station_id or None,
            date_imported=unpack_date(rec.date_imported),
            xfields=rec.xfields,
            direction_units=_libtcd.get_dir_units(rec.direction_units),
            min_direction=unpack_dir(rec.min_direction),
            max_direction=unpack_dir(rec.max_direction),
            level_units=_libtcd.get_level_units(rec.level_units),
            )

    def _reference_station(self, rec):
        assert rec.header.record_type == _libtcd.REFERENCE_STATION
        assert rec.header.reference_station == -1

        coefficients = []
        for n, constituent in enumerate(self.constituents):
            amplitude = rec.amplitude[n]
            if amplitude != 0.0:
                epoch = rec.epoch[n]
                coefficients.append(Coefficient(amplitude, epoch, constituent))

        return ReferenceStation(
            datum_offset=rec.datum_offset,
            datum=_libtcd.get_datum(rec.datum),
            zone_offset=unpack_offset(rec.zone_offset),
            expiration_date=unpack_date(rec.expiration_date),
            months_on_station=rec.months_on_station or None,
            last_date_on_station=unpack_date(rec.last_date_on_station),
            confidence=rec.confidence,
            coefficients=coefficients,
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
            min_level_multiply=level_multiply_(rec.min_level_multiply),
            max_time_add=unpack_offset(rec.max_time_add),
            max_level_add=rec.max_level_add,
            max_level_multiply=level_multiply_(rec.max_level_multiply),
            flood_begins=unpack_offset(rec.flood_begins,
                                       _libtcd.NULLSLACKOFFSET),
            ebb_begins=unpack_offset(rec.ebb_begins, _libtcd.NULLSLACKOFFSET),
            **self._common_attrs(rec))

    def _record(self, station):
        header = _libtcd.TIDE_STATION_HEADER(
            latitude=station.latitude,
            longitude=station.longitude,
            tzfile=_libtcd.find_or_add_tzfile(station.tzfile, self._header),
            name=station.name,
            reference_station=-1)

        dir_units = _libtcd.find_dir_units(station.direction_units)
        assert dir_units != -1          # FIXME: real exception
        level_units = _libtcd.find_level_units(station.level_units)
        assert level_units != -1        # FIXME: real exception

        rec = _libtcd.TIDE_RECORD(
            header=header,
            country=_libtcd.find_or_add_country(station.country or "Unknown",
                                                self._header),
            source=station.source or "",
            restriction=_libtcd.find_or_add_restriction(station.restriction,
                                                        self._header),
            comments=station.comments or "",
            notes=station.notes or "",
            legalese=_libtcd.find_or_add_legalese(station.legalese or "NULL",
                                                  self._header),
            station_id_context=station.station_id_context or "",
            station_id=station.station_id or "",
            date_imported=pack_date(station.date_imported),
            xfields=station.xfields or "",
            direction_units=dir_units,
            min_direction=pack_dir(station.min_direction),
            max_direction=pack_dir(station.max_direction),
            level_units=level_units,
            )

        if isinstance(station, ReferenceStation):
            rec.header.record_type = _libtcd.REFERENCE_STATION
            rec.header.reference_station = -1
            rec.datum_offset = station.datum_offset
            rec.datum = (_libtcd.find_or_add_datum(station.datum, self._header)
                         if station.datum else 0)
            rec.zone_offset = pack_offset(station.zone_offset)
            rec.expiration_date = pack_date(station.expiration_date)
            rec.months_on_station = station.months_on_station or 0
            rec.last_date_on_station = pack_date(station.last_date_on_station)
            rec.confidence = station.confidence or 0

            coeffs = dict(((c.constituent.name, c.constituent.speed), c)
                          for c in station.coefficients)
            coeff_t = c_float32 * 255
            amplitude = coeff_t()
            epoch = coeff_t()
            for n, constituent in enumerate(self.constituents):
                coeff = coeffs.pop((constituent.name, constituent.speed), None)
                if coeff is not None:
                    amplitude[n] = coeff.amplitude
                    epoch[n] = coeff.epoch
            assert len(coeffs) == 0     # FIXME: better diagnostics
            rec.amplitude = amplitude
            rec.epoch = epoch

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
            rec.min_level_multiply = station.min_level_multiply or 0
            rec.max_time_add = pack_offset(station.max_time_add)
            rec.max_level_add = station.max_level_add or 0
            rec.max_level_multiply = station.max_level_multiply or 0
            rec.flood_begins = pack_offset(station.flood_begins,
                                           _libtcd.NULLSLACKOFFSET)
            rec.ebb_begins = pack_offset(station.ebb_begins,
                                         _libtcd.NULLSLACKOFFSET)
        return rec

def unpack_date(packed):
    if packed != 0:
        year = packed // 10000
        month = (packed % 10000) // 100
        day = packed % 100
        return datetime.date(year, month, day)

def pack_date(date):
    if date is None:
        return 0
    return date.year * 10000 + date.month * 100 + date.day

def unpack_offset(packed, null_value=None):
    if packed != null_value:
        sign = 1
        if packed < 0:
            sign = -1
            packed = -packed
        hours = packed // 100
        minutes = packed % 100
        return sign * datetime.timedelta(hours=hours, minutes=minutes)

def pack_offset(offset, null_value=0):
    if offset is None:
        return null_value
    sign = 1
    if offset < datetime.timedelta(0):
        sign = -1
    minutes = int(round(abs(offset.total_seconds()) / 60.0))
    hours = minutes // 60
    minutes = minutes % 60
    return sign * (100 * hours) + minutes

def pack_dir(packed):
    if packed != 361:
        assert 0 <= packed < 360
        return packed

def unpack_dir(direction):
    if direction is None:
        return 361
    assert 0 <= direction < 360
    return direction

class Constituent(object):
    def __init__(self, start_year, name, speed, equilibriums, node_factors):
        assert len(equilibriums) == len(node_factors)
        self.start_year = start_year
        self.name = name
        self.speed = speed
        self.equilibriums = equilibriums
        self.node_factors = node_factors

    @property
    def year_end(self):
        """ One past the last year in the database.
        """
        return self.start_year + len(self.equilibriums)

    def __repr__(self):
        return "<{0.__class__.__name__}: {0.name} speed={0.speed}>".format(self)

class Coefficient(object):
    def __init__(self, amplitude, epoch, constituent):
        self.amplitude = amplitude
        self.epoch = epoch
        self.constituent = constituent

    def __repr__(self):
        return "<{0.__class__.__name__}: "\
               "{0.contituent.name} amplitude={0.amplitude}>".format(self)

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
# FIXME: move
def bytes_(s):
    if isinstance(s, text_type):
        s = s.encode('iso-8859-1')
    return s
