# encoding: utf-8
from setuptools import setup, find_packages
from motorway import version

setup(
    name = 'motorway',
    version = version,
    description = 'Real-time ZMQ-based pure python data pipeline',
    author = u'Kristian Øllegaard',
    author_email = 'kristian@plecto.com',
    zip_safe=False,
    include_package_data = True,
    packages = find_packages(exclude=[]),
    install_requires=[
        open("requirements.txt").readlines(),
    ],
)