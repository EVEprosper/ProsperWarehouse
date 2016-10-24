'''wheel setup for Prosper common utilities'''

from os import path, listdir
from setuptools import setup, find_packages

HERE = path.abspath(path.dirname(__file__))

def include_all_subfiles(path_included):
    '''for data_files {path_included}/*'''
    local_path = path.join(HERE, path_included)
    file_list = []

    for file in listdir(local_path):
        file_list.append(path_included + '/' + file)

    return file_list

def hack_find_packages(include_str):
    '''setuptools.find_packages({include_str}) does not work.  Adjust pathing'''
    new_list = [include_str]
    for element in find_packages(include_str):
        new_list.append(include_str + '.' + element)

    return new_list

setup(
    name='ProsperWarehouse',
    author='John Purcell',
    author_email='prospermarketshow@gmail.com',
    url='https://github.com/EVEprosper/ProsperWarehouse',
    download_url='https://github.com/EVEprosper/ProsperWarehouse/tarball/v0.0.3',
    version='0.0.3',
    license='MIT',
    classifiers=[
        'Programming Language :: Python :: 3.5'
    ],
    keywords='prosper eveonline api database',
    packages=hack_find_packages('prosper'),
    data_files=[
        #TODO: license + README
        ('SQL', include_all_subfiles('SQL')),
        ('docs', include_all_subfiles('docs'))
    ],
    package_data={
        'prosper':[
            'table_configs/table_config.cfg'
        ]
    },
    install_requires=[
        'configparser==3.5.0',
        'mysql-connector==2.1.4',
        'numpy==1.11.1',
        'pandas==0.18.1',
        'plumbum==1.6.2',
        'python-dateutil==2.5.3',
        'pytz==2016.6.1',
        'six==1.10.0',
        'requests==2.11.1',
        'ProsperCommon==0.2.1'
    ],
    dependency_links=[
        'https://pypi.fury.io/jyd5j4yse83c9UW64tP7/lockefox/ProsperCommon/'
    ]
)
