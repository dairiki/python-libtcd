# -*- coding: utf-8 -*-
""" Wrap xtide's libtcd.
"""
from __future__ import absolute_import

from ctypes import *

assert sizeof(c_float) == 4
c_float32 = c_float
assert sizeof(c_double) == 8
c_float64 = c_double

ONELINER_LENGTH = 90
MONOLOGUE_LENGTH = 10000
MAX_CONSTITUENTS = 255

# enum TIDE_RECORD_TYPE
REFERENCE_STATION = 1
SUBORDINATE_STATION = 2

NULLSLACKOFFSET = 0xA00
AMPLITUDE_EPSILON = 0.00005

class DB_HEADER_PUBLIC(Structure):
    _fields_ = [
        ('version', c_char * ONELINER_LENGTH),
        ('major_rev', c_uint32),
        ('minor_rev', c_uint32),
        ('last_modified', c_char * ONELINER_LENGTH),
        ('number_of_records', c_uint32),
        ('start_year', c_int32),
        ('number_of_years', c_uint32),
        ('constituents', c_uint32),
        ('level_unit_types', c_uint32),
        ('dir_unit_types', c_uint32),
        ('restriction_types', c_uint32),
        ('datum_types', c_uint32),
        ('countries', c_uint32),
        ('tzfiles', c_uint32),
        ('legaleses', c_uint32),
        ('pedigree_types', c_uint32),
        ]

class TIDE_STATION_HEADER(Structure):
    _fields_ = [
        ('record_number', c_int32),
        ('record_size', c_uint32),
        ('record_type', c_uint8),
        ('latitude', c_float64),
        ('longitude', c_float64),
        ('reference_station', c_int32),
        ('tzfile', c_int16),
        ('name', c_char * ONELINER_LENGTH),
        ]

class TIDE_RECORD(Structure):
    _fields_ = [
        ('header', TIDE_STATION_HEADER),
        ('country', c_int16),
        ('source', c_char * ONELINER_LENGTH),
        ('restriction', c_uint8),
        ('comments', c_char * MONOLOGUE_LENGTH),
        ('notes', c_char * MONOLOGUE_LENGTH),
        ('legalese', c_uint8),
        ('station_id_context', c_char * ONELINER_LENGTH),
        ('station_id', c_char * ONELINER_LENGTH),
        ('date_imported', c_uint32),
        ('xfields', c_char * MONOLOGUE_LENGTH),
        ('direction_units', c_uint8),
        ('min_direction', c_int32),
        ('max_direction', c_int32),
        ('level_units', c_uint8),

        # type 1 only
        ('datum_offset', c_float32),
        ('datum', c_int16),
        ('zone_offset', c_int32),
        ('expiration_date', c_uint32),
        ('months_on_station', c_uint16),
        ('last_date_on_station', c_uint32),
        ('confidence', c_uint8),
        ('amplitude', c_float32 * MAX_CONSTITUENTS),
        ('epoch', c_float32 * MAX_CONSTITUENTS),

        # type 2 only
        ('min_time_add', c_int32),
        ('min_level_add', c_float32),
        ('min_level_multiply', c_float32),
        ('max_time_add', c_int32),
        ('max_level_add', c_float32),
        ('max_level_multiply', c_float32),
        ('flood_begins', c_int32),
        ('ebb_begins', c_int32),
        ]


class Error(Exception):
    pass

_lib = cdll.LoadLibrary("libtcd.so.0")

def _check_bool(result, func, args):
    if not result:
        raise Error("%s failed" % func.__name__)
    return args

def _check_index(result, func, args):
    if result == -1:
        raise Error("%s failed" % func.__name__)
    return args

# FIXME: dump_tide_record
dump_tide_record = _lib.dump_tide_record
dump_tide_record.restype = None
dump_tide_record.argtypes = (POINTER(TIDE_RECORD),)

# String tables
_get_string_t = CFUNCTYPE(c_char_p, c_int32)
_find_string_t = CFUNCTYPE(c_int32, c_char_p)
for name in ('country',
             'tzfile',
             'level_units',
             'dir_units',
             'restriction',
             'datum',
             'legalese',
             'constituent',
             'station'):
    locals()['get_' + name] = _get_string_t(('get_' + name, _lib))
    locals()['find_' + name] = _find_string_t(('find_' + name, _lib))

_add_string_t = CFUNCTYPE(c_int32, c_char_p, POINTER(DB_HEADER_PUBLIC))
_add_string_paramflags = ((1, 'name'), (1, 'db', None))
for name in 'country', 'tzfile', 'restriction', 'datum', 'legalese':
    locals()['add_' + name] = _add_string_t(
        ('add_' + name, _lib), _add_string_paramflags)
    locals()['find_or_add_' + name] = _add_string_t(
        ('find_or_add_' + name, _lib), _add_string_paramflags)

_get_speed_t = CFUNCTYPE(c_float64, c_int32)
_set_speed_t = CFUNCTYPE(None, c_int32, c_float64)
get_speed = _get_speed_t(('get_speed', _lib))
set_speed = _set_speed_t(('set_speed', _lib))

_get_factor_t = CFUNCTYPE(c_float32, c_int32, c_int32)
_set_factor_t = CFUNCTYPE(None, c_int32, c_int32, c_float32)
get_equilibrium = _get_factor_t(('get_equilibrium', _lib))
set_equilibrium = _set_factor_t(('set_equilibrium', _lib))
get_node_factor = _get_factor_t(('get_node_factor', _lib))
set_node_factor = _set_factor_t(('set_node_factor', _lib))

_get_factors_t = CFUNCTYPE(POINTER(c_float32), c_int32)
get_equilibriums = _get_factors_t(('get_equilibriums', _lib))
get_node_factors = _get_factors_t(('get_node_factors', _lib))

_get_time_t = CFUNCTYPE(c_int32, c_char_p)
get_time = _get_time_t(('get_time', _lib))

_ret_time_t = CFUNCTYPE(c_char_p, c_int32)
ret_time = _ret_time_t(('ret_time', _lib))
ret_time_neat = _ret_time_t(('ret_time_neat', _lib))

_ret_date_t = CFUNCTYPE(c_char_p, c_uint32)
ret_date = _ret_date_t(('ret_date', _lib))

_search_station_t = CFUNCTYPE(c_int32, c_char_p)
search_station = _search_station_t(('search_station', _lib))


open_tide_db = _lib.open_tide_db
open_tide_db.restype = c_bool
open_tide_db.argtypes = (c_char_p,)
open_tide_db.errcheck = _check_bool

close_tide_db = _lib.close_tide_db
close_tide_db.restype = None
close_tide_db.argtypes = ()

_create_tide_db_t = CFUNCTYPE(
    c_bool,
    c_char_p,
    c_uint32, POINTER(c_char_p), POINTER(c_float64),
    c_int32, c_uint32,
    POINTER(POINTER(c_float32)), POINTER(POINTER(c_float32)))
create_tide_db = _create_tide_db_t(
    ('create_tide_db', _lib),
    ((1, 'file'),
     (1, 'constituents'), (1, 'constituent'), (1, 'speed'),
     (1, 'start_year'), (1, 'num_years'),
     (1, 'equilibrium'), (1, 'node_factor'),))
create_tide_db.errcheck = _check_bool

get_tide_db_header = _lib.get_tide_db_header
get_tide_db_header.restype = DB_HEADER_PUBLIC
get_tide_db_header.argtypes = ()



_get_partial_tide_record_t = CFUNCTYPE(c_bool,
                                       c_int32, POINTER(TIDE_STATION_HEADER))
get_partial_tide_record = _get_partial_tide_record_t(
    ('get_partial_tide_record', _lib), ((1, 'num'), (2, 'rec')))
get_partial_tide_record.errcheck = _check_bool

_get_next_partial_tide_record_t = CFUNCTYPE(c_int32,
                                            POINTER(TIDE_STATION_HEADER))
get_next_partial_tide_record = _get_next_partial_tide_record_t(
    ('get_next_partial_tide_record', _lib), ((2, 'rec'),))
# FIXME: errcheck should raise IndexError, NotFound, or StopIteration?
get_next_partial_tide_record.errcheck = _check_index

_get_nearest_partial_tide_record_t = CFUNCTYPE(c_int32,
                                               c_float64, c_float64,
                                               POINTER(TIDE_STATION_HEADER))
get_nearest_partial_tide_record = _get_nearest_partial_tide_record_t(
    ('get_nearest_partial_tide_record', _lib),
    ((1, 'lat'), (1, 'lon'), (2, 'rec')))
# FIXME: errcheck should raise IndexError, NotFound, or StopIteration?
get_nearest_partial_tide_record.errcheck = _check_index

def _check_read_tide_record(result, func, args):
    if result == -1 or result != args[0]:
        raise IndexError(args[0])
    return args
_read_tide_record_t = CFUNCTYPE(c_int32, c_int32, POINTER(TIDE_RECORD))
read_tide_record = _read_tide_record_t(('read_tide_record', _lib),
                                       ((1, 'num'), (2, 'rec')))
read_tide_record.errcheck = _check_read_tide_record


_add_tide_record_t = CFUNCTYPE(c_bool,
                               POINTER(TIDE_RECORD), POINTER(DB_HEADER_PUBLIC))
add_tide_record = _add_tide_record_t(('add_tide_record', _lib),
                                     ((1, 'rec'), (1, 'db', None)))
add_tide_record.errcheck = _check_bool

_update_tide_record_t = CFUNCTYPE(c_bool,
                                  c_int32, POINTER(TIDE_RECORD),
                                  POINTER(DB_HEADER_PUBLIC))
update_tide_record = _update_tide_record_t(
    ('update_tide_record', _lib), ((1, 'num'), (1, 'rec'), (1, 'db', None)))
update_tide_record.errcheck = _check_bool

_delete_tide_record_t = CFUNCTYPE(c_bool,
                                  c_int32, POINTER(DB_HEADER_PUBLIC))
delete_tide_record = _delete_tide_record_t(('delete_tide_record', _lib),
                                           ((1, 'num'), (1, 'db', None)))
delete_tide_record.errcheck = _check_bool

infer_constituents = _lib.infer_constituents
infer_constituents.restype = c_bool
infer_constituents.argtypes = (POINTER(TIDE_RECORD),)
infer_constituents.errcheck = _check_bool
