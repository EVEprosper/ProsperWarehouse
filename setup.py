'''wheel setup for Prosper common utilities'''

from os import path
from setuptools import setup, find_packages


HERE = path.abspath(path.dirname(__file__))

setup(
    name='ProsperWarehouse',
    version='0.0.0',
    license='MIT',
    classifiers=[
        'Programming Language :: Python :: 3.5'
    ],
    keywords='prosper eveonline api database',
    packages=find_packages(),
    package_data={
        #TODO
    },
    install_requires=[
        #TODO
    ]
)
