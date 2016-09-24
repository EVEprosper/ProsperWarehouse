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

setup(
    name='ProsperWarehouse',
    version='0.0.1',
    license='MIT',
    classifiers=[
        'Programming Language :: Python :: 3.5'
    ],
    keywords='prosper eveonline api database',
    packages=find_packages(),
    data_files=[
        ('SQL', include_all_subfiles('SQL')),
        ('docs', include_all_subfiles('docs'))
    ],
    package_data={
        'prosper':[
            'table_configs/table_config.ini'
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
    ],
    dependency_links=[
        'https://github.com/EVEprosper/ProsperWarehouse.git#egg=ProsperWarehouse' #not quite right
    ]
)
