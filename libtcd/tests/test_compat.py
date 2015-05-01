# -*- coding: utf-8 -*-
"""
"""
from __future__ import absolute_import

import pytest

from ..compat import bytes_


@pytest.mark.parametrize('s, result', [
    (u'Fü', b'F\xfc'),
    (b'\0\xff', b'\0\xff'),
    ])
def test_bytes(s, result):
    assert bytes_(s) == result


def test_bytes_raises_encode_error():
    with pytest.raises(UnicodeEncodeError):
        bytes_(u'€')
