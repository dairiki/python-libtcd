# -*- coding: utf-8 -*-
"""
"""
from __future__ import absolute_import

from six import text_type

try:
    from collections import OrderedDict ; 'SIDE-EFFECTS'
except ImportError:                     # pragma: NO COVER
    from ordereddict import OrderedDict ; 'SIDE-EFFECTS'

def bytes_(s, encoding='latin-1', errors='strict'):
    """ If ``s`` is an instance of ``text_type``, return
    ``s.encode(encoding, errors)``, otherwise return ``s``"""
    if isinstance(s, text_type): # pragma: no cover
        return s.encode(encoding, errors)
    return s
