# -*- coding: utf-8 -*-
"""
"""
from __future__ import absolute_import

import datetime
from operator import attrgetter
from threading import Lock

from six import text_type

from . import _libtcd

class Tcd(object):

    lock = Lock()
    _current_database = None

    def __init__(self, filename, constituents):
        start_year = max(map(attrgetter('start_year'), constituents))
        year_end = min(map(attrgetter('year_end'), constituents))
        num_years = year_end - start_year
        if num_years < 1:
            raise ValueError("num_years is zero")
        equilibriums = [ c.equilibriums[start_year - c.start_year:][:num_years]
                         for c in constituents ]
        node_factors = [ c.node_factors[start_year - c.start_year:][:num_years]
                         for c in constituents ]

        self.filename = filename

        with self.lock:
            type(self)._current_database = None
            rv = _libtcd.create_tide_db(
                bytes_(filename),
                len(constituents),
                map(attrgetter('name'), constituents),
                map(attrgetter('speed'), constituents),
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
            return self._station(rec)

    def __setitem__(self, i, station):
        raise NotImplementedError()     # FIXME:

    def __delitem__(self, i):
        raise NotImplementedError()     # FIXME:

    def append(self, station):
        with self:
            rec = self._record(station)
            rv = _libtcd.add_tide_record(rec, self._header)
            assert rv                   # FIXME: raise real exception

    def __iter__(self):
        for i in xrange(len(self)):
            yield self[i]

    def _read_constituent(self, num):
        start_year = self._header.start_year
        number_of_years = self._header.number_of_years
        name = _libtcd.get_constituent(num)
        speed = _libtcd.get_speed(num)
        equilibriums = [ _libtcd.get_equilibrium(num, y)
                         for y in range(number_of_years) ]
        node_factors = [ _libtcd.get_node_factor(num, y)
                         for y in range(number_of_years) ]
        return Constituent(start_year, name, speed, equilibriums, node_factors)

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
            date_imported=date_(rec.date_imported),
            xfields=rec.xfields,
            direction_units=_libtcd.get_dir_units(rec.direction_units),
            min_direction=dir_(rec.min_direction),
            max_direction=dir_(rec.max_direction),
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
            zone_offset=offset_(rec.zone_offset),
            expiration_date=date_(rec.expiration_date),
            months_on_station=rec.months_on_station or None,
            last_date_on_station=date_(rec.last_date_on_station),
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
            min_time_add=offset_(rec.min_time_add),
            min_level_add=rec.min_level_add,
            min_level_multiply=level_multiply_(rec.min_level_multiply),
            max_time_add=offset_(rec.max_time_add),
            max_level_add=rec.max_level_add,
            max_level_multiply=level_multiply_(rec.max_level_multiply),
            flood_begins=offset_(rec.flood_begins, _libtcd.NULLSLACKOFFSET),
            ebb_begins=offset_(rec.ebb_begins, _libtcd.NULLSLACKOFFSET),
            **self._common_attrs(rec))

    def _record(self, station):
        if isinstance(station, ReferenceStation):
            record_type = _libtcd.REFERENCE_STATION
            reference_station = -1
        elif isinstance(station, SubordinateStation):
            record_type = _libtcd.SUBORDINATE_STATION
            assert isinstance(station.reference_station, ReferenceStation)
            #FIXME:
            raise NotImplementedError()

        header = _libtcd.TIDE_STATION_HEADER(
            latitude=station.latitude,
            longitude=station.longitude,
            tzfile=_libtcd.find_or_add(station.tzfile),
            name=station.name,
            record_type=record_type,
            reference_station=reference_station)

        return _libtcd.TIDE_RECORD(
            header=header,
            country=_libtcd.find_or_add_country(station.country or "Unknown"),
            source=station.source or "",
            restriction=_libtcd.find_or_add_restriction(station.restriction),
            )
        return dict(
            country=_libtcd.get_country(rec.country),
            source=rec.source or None,
            restriction=_libtcd.get_restriction(rec.restriction),
            comments=rec.comments or None,
            notes=rec.notes,
            legalese=(_libtcd.get_legalese(rec.legalese)
                      if rec.legalese != 0 else None),
            station_id_context=rec.station_id_context or None,
            station_id=rec.station_id or None,
            date_imported=date_(rec.date_imported),
            xfields=rec.xfields,
            direction_units=_libtcd.get_dir_units(rec.direction_units),
            min_direction=dir_(rec.min_direction),
            max_direction=dir_(rec.max_direction),
            level_units=_libtcd.get_level_units(rec.level_units),
            )

def date_(packed):
    if packed != 0:
        year = packed // 10000
        month = (packed % 10000) // 100
        day = packed % 100
        return datetime.date(year, month, day)

def offset_(packed, null_value=None):
    if packed != null_value:
        sign = 1
        if packed < 0:
            sign = -1
            packed = -packed
        hours = packed // 100
        minutes = packed % 100
        return sign * datetime.timedelta(hours=hours, minutes=minutes)

def dir_(packed):
    if packed != 361:
        assert 0 <= packed < 360
        return packed

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
