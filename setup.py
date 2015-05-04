# -*- coding: utf-8 -*-
import os
import sys

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

VERSION = '0.1a1'

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
CHANGES = open(os.path.join(here, 'CHANGES.rst')).read()

install_requires = ['six']

tests_require = ['pytest']

if sys.version_info < (2, 7):
    install_requires.append('ordereddict')


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)

setup(name='libtcd',
      version=VERSION,
      description='Interface to xtide’s libtcd',
      long_description=README + '\n\n' + CHANGES,
      classifiers=[
          "Development Status :: 3 - Alpha",
          "Intended Audience :: Developers",
          "License :: OSI Approved :: BSD License",
          "Programming Language :: Python",
          "Programming Language :: Python :: 2.6",
          "Programming Language :: Python :: 2.7",
          "Programming Language :: Python :: 3.3",
          "Programming Language :: Python :: 3.4",
          "Programming Language :: Python :: Implementation :: CPython",
          "Topic :: Scientific/Engineering",
          "Topic :: Software Development :: Libraries",
          ],
      author='Jeff Dairiki',
      author_email='dairiki@dairiki.org',
      url='https://github.com/dairiki/python-libtcd',
      keywords='xtide tides tidal currents',

      cmdclass={'test': PyTest},

      packages=find_packages(),
      install_requires=install_requires,
      include_package_data=True,
      zip_safe=True,

      tests_require=tests_require,
      )
