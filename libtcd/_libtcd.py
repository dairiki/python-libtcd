# -*- coding: utf-8 -*-
""" Wrap xtide's libtcd.
"""
from __future__ import absolute_import

from ctypes import (
    c_bool,
    c_char,
    c_char_p,
    c_double,
    c_float,
    c_int16,
    c_int32,
    c_uint8,
    c_uint16,
    c_uint32,
    cdll,
    sizeof,
    Structure,
    CFUNCTYPE,
    POINTER,
    )

ENCODING = 'iso-8859-1'         # all strings encoded iso-8859-1

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
    _anonymous_ = ['header']
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


_marker = object()


class _Param(object):
    """ Marker for parameter types. """
    direction_flag = 1          # input parameter, by default

    def __init__(self, typ, name=None, default=_marker):
        self.typ = typ
        if default is not _marker:
            self.paramflag = (self.direction_flag, name, default)
        elif name:
            self.paramflag = (self.direction_flag, name)
        else:
            self.paramflag = (self.direction_flag,)


class _OutputParam(_Param):
    direction_flag = 2          # output parameter


def _to_param(param_or_type):
    if isinstance(param_or_type, _Param):
        return param_or_type
    return _Param(param_or_type)


def _declare(name, *params, **kwargs):
    params = list(map(_to_param, params))
    argtypes = tuple(param.typ for param in params)
    paramflags = tuple(param.paramflag for param in params)
    restype = kwargs.get('restype')
    errcheck = kwargs.get('errcheck')
    func = CFUNCTYPE(restype, *argtypes)((name, _lib), paramflags)
    func.__name__ = name
    if errcheck:
        func.errcheck = errcheck
    globals()[name] = func

_declare('dump_tide_record', _Param(POINTER(TIDE_RECORD), 'rec'))

# String tables
for _name in ('country',
              'tzfile',
              'level_units',
              'dir_units',
              'restriction',
              'datum',
              'legalese',
              'constituent',
              'station'):
    _declare('get_' + _name, c_int32, restype=c_char_p)
    _declare('find_' + _name, c_char_p, restype=c_int32)
for _name in 'country', 'tzfile', 'restriction', 'datum', 'legalese':
    for _pfx in ('add_', 'find_or_add_'):
        _declare(_pfx + _name,
                 _Param(c_char_p, 'name'),
                 _Param(POINTER(DB_HEADER_PUBLIC), 'db', default=None),
                 restype=c_int32)

_declare('get_speed', c_int32, restype=c_float64)
_declare('set_speed', c_int32, c_float64)

_declare('get_equilibrium', c_int32, c_int32, restype=c_float32)
_declare('set_equilibrium', c_int32, c_int32, c_float32)
_declare('get_node_factor', c_int32, c_int32, restype=c_float32)
_declare('set_node_factor', c_int32, c_int32, c_float32)

_declare('get_equilibriums', c_int32, restype=POINTER(c_float32))
_declare('get_node_factors', c_int32, restype=POINTER(c_float32))

_declare('get_time', c_char_p, restype=c_int32)
_declare('ret_time', c_int32, restype=c_char_p)
_declare('ret_time_neat', c_int32, restype=c_char_p)
_declare('ret_date', c_uint32, restype=c_char_p)

_declare('search_station', c_char_p, restype=c_int32)

_declare('open_tide_db', c_char_p, restype=c_bool, errcheck=_check_bool)
_declare('close_tide_db')
_declare('create_tide_db',
         _Param(c_char_p, 'file'),
         _Param(c_uint32, 'constituents'),
         _Param(POINTER(c_char_p), 'constituent'),
         _Param(POINTER(c_float64), 'speed'),
         _Param(c_int32, 'start_year'),
         _Param(c_uint32, 'num_years'),
         _Param(POINTER(POINTER(c_float32)), 'equilibrium'),
         _Param(POINTER(POINTER(c_float32)), 'node_factor'),
         restype=c_bool, errcheck=_check_bool)

_declare('get_tide_db_header', restype=DB_HEADER_PUBLIC)


def _check_return_none_on_failure(result, func, args):
    if isinstance(result, bool):
        success = result
    else:
        success = result >= 0           # result is index or -1
    rval = args[-1]
    return rval if success else None

_declare('get_partial_tide_record',
         _Param(c_int32, 'num'),
         _OutputParam(POINTER(TIDE_STATION_HEADER)),
         restype=c_bool, errcheck=_check_return_none_on_failure)
_declare('get_next_partial_tide_record',
         _OutputParam(POINTER(TIDE_STATION_HEADER)),
         restype=c_int32, errcheck=_check_return_none_on_failure)
_declare('get_nearest_partial_tide_record',
         _Param(c_float64, 'lat'),
         _Param(c_float64, 'lon'),
         _OutputParam(POINTER(TIDE_STATION_HEADER)),
         restype=c_int32, errcheck=_check_return_none_on_failure)

_declare('read_tide_record',
         _Param(c_int32, 'num'),
         _OutputParam(POINTER(TIDE_RECORD)),
         restype=c_int32, errcheck=_check_return_none_on_failure)
_declare('read_next_tide_record',
         _OutputParam(POINTER(TIDE_RECORD)),
         restype=c_int32, errcheck=_check_return_none_on_failure)
_declare('add_tide_record',
         _Param(POINTER(TIDE_RECORD), 'rec'),
         _Param(POINTER(DB_HEADER_PUBLIC), 'db', default=None),
         restype=c_bool, errcheck=_check_bool)
_declare('update_tide_record',
         _Param(c_int32, 'num'),
         _Param(POINTER(TIDE_RECORD), 'rec'),
         _Param(POINTER(DB_HEADER_PUBLIC), 'db', default=None),
         restype=c_bool, errcheck=_check_bool)
_declare('delete_tide_record',
         _Param(c_int32, 'num'),
         _Param(POINTER(DB_HEADER_PUBLIC), 'db', default=None),
         restype=c_bool, errcheck=_check_bool)

_declare('infer_constituents',
         POINTER(TIDE_RECORD),
         restype=c_bool, errcheck=_check_bool)
