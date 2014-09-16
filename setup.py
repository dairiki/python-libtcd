# -*- coding: utf-8 -*-
import os
import sys

from setuptools import setup, find_packages

VERSION = '0.1.dev0'

#here = os.path.abspath(os.path.dirname(__file__))
#README = open(os.path.join(here, 'README.rst')).read()
#CHANGES = open(os.path.join(here, 'CHANGES.rst')).read()

requires = [
    'six',
    ]

setup(name='libtcd',
      version=VERSION,
      description='Interface to xtide’s libtcd',
     #long_description=README + '\n\n' + CHANGES,
      classifiers=[
        "Programming Language :: Python",
        # FIXME
        ],
      author='Jeff Dairiki',
      author_email='dairiki@dairiki.org',
      url='',
      keywords='xtide',

      packages=find_packages(),
      install_requires=requires,
      include_package_data=True,
      zip_safe=True,

      #tests_require=tests_require,
      #test_suite="btp_helpers.tests",
      )
