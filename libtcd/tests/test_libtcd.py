# -*- coding: utf-8 -*-
"""
"""
from __future__ import absolute_import

from pkg_resources import resource_filename

import pytest


@pytest.fixture
def test_tcd():
    from libtcd._libtcd import open_tide_db, get_tide_db_header
    harmonics_tcd = resource_filename('libtcd.tests', 'harmonics-initial.tcd')
    open_tide_db(harmonics_tcd)
    return get_tide_db_header()

def test_get_tide_db_header(test_tcd):
    assert 'v2.2' in test_tcd.version
    assert test_tcd.major_rev == 2
    assert test_tcd.minor_rev == 2
    assert test_tcd.number_of_records == 2
    assert test_tcd.start_year == 1970
    assert test_tcd.number_of_years == 68

def test_get_level_units(test_tcd):
    from libtcd._libtcd import get_level_units
    level_units = map(get_level_units, range(test_tcd.level_unit_types))
    assert level_units == ['Unknown', 'feet', 'meters', 'knots', 'knots^2']

def test_get_dir_units(test_tcd):
    from libtcd._libtcd import get_dir_units
    dir_units = map(get_dir_units, range(test_tcd.dir_unit_types))
    assert dir_units == ['Unknown', 'degrees true', 'degrees']

def test_get_restrictions(test_tcd):
    from libtcd._libtcd import get_restriction
    restrictions = map(get_restriction, range(test_tcd.restriction_types))
    assert restrictions == ['Public Domain',
                            'DoD/DoD Contractors Only',
                            'Non-commercial use only']
