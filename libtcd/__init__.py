# -*- coding: utf-8 -*-
""" Wrap xtide's libtcd.
"""
from __future__ import absolute_import

from . import _libtcd

# from _libtcd import (
#     # FIXME: more?
#     Error,
#     REFERENCE_STATION,
#     SUBORDINATE_STATION,
#     )
# def dump_struct(s):
#     for name, typ in type(s)._fields_:
#         print(name, getattr(s, name))

# _libtcd.open_tide_db("/usr/share/xtide/harmonics-dwf-20100529-free.tcd")

# db = _libtcd.get_tide_db_header()
# dump_struct(db)

# print
# #rec = TIDE_RECORD()
# for i in range(100000):
#     rec = _libtcd.read_tide_record(i)
#     if rec.header.record_type == _libtcd.SUBORDINATE_STATION:
#         dump_struct(rec.header)
#         print
#         dump_struct(rec)
#         break

#from libtcd.api import Tcd

#db = Tcd("/usr/share/xtide/harmonics-dwf-20100529-free.tcd")
#print db.level_units
