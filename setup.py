#  -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name='o3',
    packages=find_packages(include=('o3',)),
    url='https://github.com/mblomdahl/o3',
    license='The Unlicense',
    description='Hadoop-Airflow analytics',
    entry_points={
        'console_scripts': [
            'o3-cli = o3_cli:o3_cli'
        ]
    },
    # https://pypi.python.org/pypi?:action=list_classifiers
    install_requires=[
        'psycopg2',
        'hdfs3',
        'apache-airflow[password,ssh]',
        'ansible',
        'dictdiffer',
        'netaddr',
        'ipython',
        'jupyter',
        'pandas',
        'fastavro',
        'pyhive',
        'pyspark'
    ],
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.6'
    ],
    platforms=['POSIX']
)
