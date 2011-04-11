from setuptools import setup, find_packages
import sys, os

version = '0.0'

setup(name='py-tpcc',
      version=version,
      description="Python implementation of the TPC-C benchmark",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Andy Pavlo',
      author_email='pavlo@cs.brown.edu',
      url='http://www.cs.brown.edu/~pavlo/',
      license='BSD',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          # -*- Extra requirements: -*-
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
