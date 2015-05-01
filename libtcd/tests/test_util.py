# -*- coding: utf-8 -*-
"""
"""
from __future__ import absolute_import

from datetime import timedelta
import unittest

import pytest


class Test_timedelta_total_minutes(object):
    def call_it(self, td, *args, **kwargs):
        from libtcd.util import timedelta_total_minutes
        return timedelta_total_minutes(td, *args, **kwargs)

    @pytest.mark.parametrize("td,expected", [
        (timedelta(), 0),
        (timedelta(hours=1), 60),
        (timedelta(seconds=29.99), 0),
        (timedelta(seconds=30.01), 1),
        (timedelta(seconds=-29.99), 0),
        (timedelta(seconds=-30.01), -1),
        (timedelta(hours=-2), -120),
        ])
    def test(self, td, expected):
        assert self.call_it(td) == expected

    @pytest.mark.parametrize(
        "td", [timedelta(microseconds=1), timedelta(seconds=1)])
    def test_strict_raises_valueerror(self, td):
        with pytest.raises(ValueError):
            self.call_it(td, strict=True)

    @pytest.mark.parametrize("td,expected", [
        (timedelta(), 0),
        (timedelta(minutes=1), 1),
        (timedelta(hours=-1), -60),
        ])
    def test_strict(self, td, expected):
        assert self.call_it(td, strict=True) == expected


class Test_remove_if_exists(object):
    def call_it(self, filename):
        from libtcd.util import remove_if_exists
        return remove_if_exists(filename)

    def test_removes_file(self, tmpdir):
        testfile = tmpdir.ensure('testfile')
        assert testfile.isfile()
        self.call_it(testfile.strpath)
        assert not testfile.exists()

    def test__is_okay_with_missing_file(self, tmpdir):
        testfile = tmpdir.join('testfile')
        assert not testfile.exists()
        self.call_it(testfile.strpath)

    def test_remove_if_exists_raises_other_errors(self, tmpdir):
        testdir = tmpdir.mkdir('testdir')
        with pytest.raises(OSError):
            self.call_it(testdir.strpath)


class TestReify(unittest.TestCase):
    # Ripped verbatim from pyramid.tests.test_decorator
    def _makeOne(self, wrapped):
        from ..util import reify
        return reify(wrapped)

    def test___get__withinst(self):
        def wrapped(inst):
            return 'a'
        decorator = self._makeOne(wrapped)
        inst = Dummy()
        result = decorator.__get__(inst)
        self.assertEqual(result, 'a')
        self.assertEqual(inst.__dict__['wrapped'], 'a')

    def test___get__noinst(self):
        decorator = self._makeOne(None)
        result = decorator.__get__(None)
        self.assertEqual(result, decorator)

    def test___doc__copied(self):
        def wrapped(inst):
            """My doc"""
        decorator = self._makeOne(wrapped)
        self.assertEqual(decorator.__doc__, "My doc")


class Dummy(object):
    pass
