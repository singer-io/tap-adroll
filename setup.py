#!/usr/bin/env python

from setuptools import setup

setup(name='tap-adroll',
      version='1.0.2',
      description='Singer.io tap for extracting data from Adroll',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_adroll'],
      install_requires=[
          'requests==2.23.0',
          'requests_oauthlib==1.3.0',
          'singer-python==5.9.0',
      ],
      extras_require={
          'dev': [
              'pylint==2.5.3',
              'ipdb',
              'nose',
          ],
          'test': [
              'simplejson==3.11.1',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-adroll=tap_adroll:main
      ''',
      packages=['tap_adroll'],
      package_data = {
          "tap_adroll": ["tap_adroll/schemas/*.json"]
      },
      include_package_data=True,
)
