##########################
Python Bindings for Libtcd
##########################

This is a set of ctypes_ bindings to ``libtcd.so``.
The raw bindings are in ``libtcd._libtcd``, and a higher level interface to these can be found in ``libtcd.api``.

Libtcd_ is the library used to read and write files containing harmonic
constituents and secondary station offsets used by the xtide_
program to predict tidal heights and currents.

Requirements
============

You must have ``libtcd.so.0``, the shared library for libtcd_ installed
on your system.

This code has been tested under CPython 2.6, 2.7, 3.2 and 3.4.

Development
===========

Development takes place at https://github.com/dairiki/python-libtcd

Author
======

Jeff Dairiki <dairiki@dairiki.org>

.. _ctypes: https://docs.python.org/library/ctypes.html
.. _xtide: http://xtide.org/xtide/
.. _libtcd: http://xtide.org/xtide/libtcd.html
