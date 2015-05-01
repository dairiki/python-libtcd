# -*- coding: utf-8 -*-
"""
"""
from __future__ import absolute_import

from six import text_type

try:
    from collections import OrderedDict  # noqa
except ImportError:                      # pragma: NO COVER
    from ordereddict import OrderedDict  # noqa


def bytes_(s, encoding='latin-1', errors='strict'):
    """ If ``s`` is an instance of ``text_type``, return
    ``s.encode(encoding, errors)``, otherwise return ``s``"""
    if isinstance(s, text_type):
        return s.encode(encoding, errors)
    return s
