# encoding: utf-8
from setuptools import setup, find_packages
from motorway import version

setup(
    name = 'livestats-data-pipeline',
    version = version,
    description = '',
    author = u'Kristian Øllegaard',
    author_email = 'kristian@livesystems.info',
    zip_safe=False,
    include_package_data = True,
    packages = find_packages(exclude=[]),
    install_requires=[
        open("requirements.txt").readlines(),
    ],
)