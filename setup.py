#!/usr/bin/env python

from setuptools import setup

setup(name='tap-adroll',
      version='0.0.1',
      description='Singer.io tap for extracting data from Adroll',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_adroll'],
      install_requires=[
          'requests==2.23.0',
          'singer-python==5.9.0',
      ],
      extras_require={
          'dev': [
              'pylint',
              'ipdb',
              'nose',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-adroll=tap_adroll:main
      ''',
      packages=['tap_adroll'],
      package_data = {
          'tap_adroll/schemas': [
              # add schema.json filenames here
          ]
      },
      include_package_data=True,
)
