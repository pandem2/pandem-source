# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

setup(
    name='pandem-source',
    version='0.0.7',
    description='Manage heterogeneous data sources for PANDEM-2 project',
    long_description=readme,
    long_description_content_type='text/markdown',
    author='Francisco Orchard',
    author_email='f.orchard@epiconcept.fr',
    url='https://github.com/pandem2/pandem-source',
    license="EUPL-1.2",
    install_requires=[
      "pyyaml",
      "pandas",
      "openpyxl",
      "pykka",
      "lxml",
      "isoweek",
      "tweepy",
      "tornado",
      "asyncio",
      "tornado-rest-swagger"
    ],
    packages=find_packages(exclude=('tests', 'docs')),
    include_package_data=True
)

